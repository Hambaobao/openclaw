// Register lifecycle mocks before the real monitor and bot consume their modules.
// oxfmt-ignore
import { getFeishuLifecycleTestMocks, resetFeishuLifecycleTestMocks } from "./lifecycle.test-support.js";
import { EventDispatcher } from "@larksuiteoapi/node-sdk";
import { createInboundDebouncer } from "openclaw/plugin-sdk/channel-inbound-debounce";
import { createChannelIngressQueueForTests } from "openclaw/plugin-sdk/channel-ingress-test-runtime";
import { createDeferred } from "openclaw/plugin-sdk/extension-shared";
import { createPluginStateKeyedStoreForTests } from "openclaw/plugin-sdk/plugin-state-test-runtime";
import { createNonExitingRuntimeEnv } from "openclaw/plugin-sdk/plugin-test-runtime";
import { closeOpenClawStateDatabaseAsync } from "openclaw/plugin-sdk/sqlite-runtime-testing";
import { withOpenClawTestState } from "openclaw/plugin-sdk/test-state";
import { expect, it, vi } from "vitest";
import type { PluginRuntime } from "../runtime-api.js";
import { resolveFeishuAccount } from "./accounts.js";
import { feishuDedupeState } from "./dedup-state.js";
import { monitorSingleAccount } from "./monitor.account.js";
import * as processing from "./processing-indicator.js";
import * as reactions from "./reactions.js";
import {
  createFeishuLifecycleConfig,
  createFeishuLifecycleReplyDispatcher,
  installFeishuLifecycleReplyRuntime,
} from "./test-support/lifecycle-test-support.js";

type ReplyOptions = NonNullable<
  Parameters<PluginRuntime["channel"]["reply"]["dispatchReplyFromConfig"]>[0]["replyOptions"]
>;
type TurnLifecycle = NonNullable<ReplyOptions["turnAdoptionLifecycle"]>;

it.each([
  { chatType: "p2p", preparationMs: 0 },
  { chatType: "group", preparationMs: 0 },
  { chatType: "group", preparationMs: 135_906 },
])(
  "keeps registered $chatType Wait through $preparationMs ms before Typing and exact cleanup",
  async ({ chatType, preparationMs }) => {
    await withOpenClawTestState({ label: "feishu-registered-processing" }, async (state) => {
      vi.useFakeTimers();
      resetFeishuLifecycleTestMocks();
      const mocks = getFeishuLifecycleTestMocks();
      const cfg = createFeishuLifecycleConfig({
        accountId: "test",
        appId: "fixture-app",
        appSecret: "fixture-secret", // pragma: allowlist secret
        channelConfig: {
          dmPolicy: "open",
          allowFrom: ["*"],
          groupPolicy: "open",
          groups: { "oc-group": { requireMention: false, groupSessionScope: "group_topic" } },
        },
      });
      mocks.createEventDispatcherMock.mockImplementation(() => new EventDispatcher({}));
      mocks.createFeishuReplyDispatcherMock.mockImplementation(
        createFeishuLifecycleReplyDispatcher,
      );
      mocks.resolveAgentRouteMock.mockReturnValue({
        agentId: "main",
        channel: "feishu",
        accountId: "test",
        sessionKey:
          chatType === "group"
            ? "agent:main:feishu:group:oc-group:topic:om-topic-root"
            : "agent:main:feishu:dm:ou-user",
        mainSessionKey: "agent:main:main",
        lastRoutePolicy: "session",
        matchedBy: "default",
      });
      const accepted = createDeferred<TurnLifecycle>();
      let acceptedLifecycle: TurnLifecycle | undefined;
      const completion = createDeferred<void>();
      const coreAcceptance = vi.fn<PluginRuntime["channel"]["reply"]["dispatchReplyFromConfig"]>(
        async ({ replyOptions }) => {
          const lifecycle = replyOptions?.turnAdoptionLifecycle;
          if (!lifecycle?.onDeferred) {
            throw new Error("Registered message dispatch did not carry its ingress lifecycle");
          }
          lifecycle.onDeferred();
          acceptedLifecycle = lifecycle;
          accepted.resolve(lifecycle);
          return {
            queuedFinal: false,
            counts: { tool: 0, block: 0, final: 0 },
            noVisibleReplyFallbackEligible: false,
          };
        },
      );
      const runtime = installFeishuLifecycleReplyRuntime({
        resolveAgentRouteMock: mocks.resolveAgentRouteMock,
        dispatchReplyFromConfigMock: coreAcceptance,
        withReplyDispatcherMock: mocks.withReplyDispatcherMock,
        storePath: `${state.stateDir}/sessions.json`,
      });
      runtime.config.current = () => cfg;
      runtime.channel.debounce.createInboundDebouncer = createInboundDebouncer;
      runtime.state.openChannelIngressQueue = () =>
        createChannelIngressQueueForTests({
          channelId: "feishu",
          accountId: "test",
          stateDir: state.stateDir,
        });
      runtime.state.openKeyedStore = <T>(
        options: Parameters<PluginRuntime["state"]["openKeyedStore"]>[0],
      ) => createPluginStateKeyedStoreForTests<T>("feishu", { ...options, env: state.env });

      const calls: Array<{
        operation: "add" | "delete";
        messageId: string;
        emoji?: string;
        reactionId?: string;
      }> = [];
      const active = new Map<string, string>();
      vi.spyOn(reactions, "addReactionFeishu").mockImplementation(
        async ({ messageId, emojiType }) => {
          const reactionId = `receipt-${emojiType}`;
          calls.push({ operation: "add", messageId, emoji: emojiType });
          active.set(reactionId, emojiType);
          return { reactionId };
        },
      );
      vi.spyOn(reactions, "removeReactionFeishu").mockImplementation(
        async ({ messageId, reactionId }) => {
          calls.push({ operation: "delete", messageId, reactionId });
          active.delete(reactionId);
        },
      );
      let owner: ReturnType<typeof processing.createFeishuProcessingIndicator> | undefined;
      const createOwner = processing.createFeishuProcessingIndicator;
      vi.spyOn(processing, "createFeishuProcessingIndicator").mockImplementation((options) => {
        owner = createOwner(options);
        return owner;
      });
      const controller = new AbortController();
      const ready = createDeferred<EventDispatcher>();
      mocks.monitorWebSocketMock.mockImplementation(async (...args: unknown[]) => {
        const transport = args[0] as { eventDispatcher: EventDispatcher };
        ready.resolve(transport.eventDispatcher);
        await new Promise<void>((resolve) => {
          controller.signal.addEventListener("abort", () => resolve(), { once: true });
        });
      });
      const monitor = monitorSingleAccount({
        cfg,
        account: resolveFeishuAccount({ cfg, accountId: "test" }),
        runtime: createNonExitingRuntimeEnv(),
        abortSignal: controller.signal,
        fireAndForget: false,
        botOpenIdSource: { kind: "prefetched", botOpenId: "ou-bot", source: "provider" },
      });
      void monitor.catch(ready.reject);
      let admission: TurnLifecycle | undefined;
      let eventDelivery: Promise<unknown> | undefined;
      try {
        const dispatcher = await ready.promise;
        const processingOwner = owner;
        if (!processingOwner) {
          throw new Error("Account monitor did not create its processing owner");
        }
        eventDelivery = dispatcher.invoke({
          schema: "2.0",
          header: { event_id: "evt-registered-processing", event_type: "im.message.receive_v1" },
          event: {
            sender: { sender_id: { open_id: "ou-user" }, sender_type: "user" },
            message: {
              message_id: "om-registered-processing",
              chat_id: chatType === "group" ? "oc-group" : "oc-direct",
              chat_type: chatType,
              ...(chatType === "group"
                ? { root_id: "om-topic-root", thread_id: "omt-processing" }
                : {}),
              message_type: "text",
              create_time: String(Date.now()),
              content: JSON.stringify({ text: "registered processing feedback" }),
            },
          },
        });
        void eventDelivery.catch(() => undefined);
        await vi.waitFor(() => expect(acceptedLifecycle).toBeDefined(), { timeout: 5_000 });
        await eventDelivery;
        admission = await accepted.promise;
        await processingOwner.drain();
        expect([...active.values()]).toEqual(["OneSecond"]);

        await vi.advanceTimersByTimeAsync(preparationMs);
        await processingOwner.drain();
        expect([...active.values()]).toEqual(["OneSecond"]);
        expect(calls).toEqual([
          { operation: "add", messageId: "om-registered-processing", emoji: "OneSecond" },
        ]);

        await admission.onAdopted();
        admission.onRunIncluded?.({ runId: "registered-run", completion: completion.promise });
        await processingOwner.drain();
        expect([...active.values()]).toEqual(["Typing"]);

        completion.resolve();
        await processingOwner.drain();
        expect(active.size).toBe(0);
        expect(calls).toEqual([
          { operation: "add", messageId: "om-registered-processing", emoji: "OneSecond" },
          {
            operation: "delete",
            messageId: "om-registered-processing",
            reactionId: "receipt-OneSecond",
          },
          { operation: "add", messageId: "om-registered-processing", emoji: "Typing" },
          {
            operation: "delete",
            messageId: "om-registered-processing",
            reactionId: "receipt-Typing",
          },
        ]);
        expect(coreAcceptance).toHaveBeenCalledOnce();
      } finally {
        completion.resolve();
        await admission?.onAdopted();
        vi.useRealTimers();
        controller.abort();
        await eventDelivery?.catch(() => undefined);
        await monitor;
        await closeOpenClawStateDatabaseAsync();
        feishuDedupeState.reset();
        vi.restoreAllMocks();
        vi.useRealTimers();
      }
    });
  },
);
