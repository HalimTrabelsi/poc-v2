/** @odoo-module **/

import { registry } from "@web/core/registry";
import { KanbanController } from "@web/views/kanban/kanban_controller";
import { KanbanRenderer } from "@web/views/kanban/kanban_renderer";
import { kanbanView } from "@web/views/kanban/kanban_view";
import { useService } from "@web/core/utils/hooks";
import { onMounted, onWillUnmount } from "@odoo/owl";

class FraudLiveMonitorController extends KanbanController {
    setup() {
        super.setup();

        this.busService = this.env.services.bus_service;
        this.notification = useService("notification");
        this.user = useService("user");
        this.rpc = useService("rpc");

        this._busHandler = this._onBusNotification.bind(this);
        this._tickInterval = null;
        this._refreshInterval = null;
        this._cronNextRunAt = null;
        this._scanning = false;

        onMounted(() => {
            if (this.busService) {
                const userChannel = `res.partner-${this.user.partnerId}`;
                this.busService.addChannel(userChannel);
                this.busService.addEventListener("notification", this._busHandler);
            }
            this._mountCronBanner();
            this._refreshCronStatus();
            this._tickInterval = setInterval(() => this._tickCronBanner(), 1000);
            this._refreshInterval = setInterval(() => this._refreshCronStatus(), 15000);
        });

        onWillUnmount(() => {
            if (this.busService) {
                const userChannel = `res.partner-${this.user.partnerId}`;
                this.busService.removeEventListener("notification", this._busHandler);
                this.busService.deleteChannel(userChannel);
            }
            clearInterval(this._tickInterval);
            clearInterval(this._refreshInterval);
        });
    }

    // ── Live cron-scan timer banner ─────────────────────────────────────────
    // Injected via plain DOM (rather than overriding the Kanban Renderer's
    // QWeb template, which would need a separate template-registration file)
    // so the "1-minute sync cron is really running" claim is visibly proven
    // — real last-scan/next-scan timestamps from /fraud/cron_status, ticking
    // client-side between periodic re-fetches.
    _mountCronBanner() {
        const root = this.rootRef.el ? this.rootRef.el.querySelector(".o_kanban_renderer") : null;
        const container = (root && root.parentElement) || this.rootRef.el;
        if (!container || container.querySelector(".fraud-cron-banner")) {
            return;
        }
        const banner = document.createElement("div");
        banner.className = "fraud-cron-banner";
        banner.innerHTML = `
            <span class="fraud-cron-title"><span class="fraud-cron-dot"></span>Auto-scan cron</span>
            <span class="fraud-cron-sep">|</span>
            <span class="fraud-cron-last">Last scan: —</span>
            <span class="fraud-cron-sep">|</span>
            <span class="fraud-cron-chrono">
                <span class="fraud-cron-chrono-label">Next scan</span>
                <span class="fraud-cron-chrono-value">--:--</span>
            </span>
            <span class="fraud-cron-count">+0 new</span>
        `;
        container.insertBefore(banner, container.firstChild);
        this._bannerEl = banner;
    }

    async _refreshCronStatus() {
        try {
            const data = await this.rpc("/fraud/cron_status", {});
            this._cronNextRunAt = data.next_run_at || null;
            if (this._bannerEl) {
                const lastEl = this._bannerEl.querySelector(".fraud-cron-last");
                if (lastEl) {
                    const label = data.last_sync_at
                        ? new Date(data.last_sync_at.replace(" ", "T") + "Z").toLocaleTimeString()
                        : "never yet";
                    lastEl.textContent = `Last scan: ${label}`;
                }
                const countEl = this._bannerEl.querySelector(".fraud-cron-count");
                if (countEl) {
                    countEl.textContent = `+${data.last_sync_count || 0} new`;
                }
            }
            this._tickCronBanner();
        } catch {
            // Non-fatal — the kanban view itself still works without the timer.
        }
    }

    _tickCronBanner() {
        if (!this._bannerEl) {
            return;
        }
        const valueEl = this._bannerEl.querySelector(".fraud-cron-chrono-value");
        if (!valueEl) {
            return;
        }
        if (!this._cronNextRunAt) {
            valueEl.textContent = "--:--";
            return;
        }
        const next = new Date(this._cronNextRunAt.replace(" ", "T") + "Z").getTime();
        const diff = Math.max(0, Math.round((next - Date.now()) / 1000));

        if (diff <= 0) {
            // The cron's scheduled minute has arrived — flip to a "scanning"
            // state and force a real reload (kanban records + cron status)
            // so the scan is visibly proven even when it finds nothing new,
            // rather than only reloading on a new_alert bus notification.
            if (!this._scanning) {
                this._scanning = true;
                valueEl.textContent = "●";
                this._bannerEl.classList.add("fraud-cron-scanning");
                if (this.model) {
                    this.model.load();
                }
                setTimeout(() => {
                    this._refreshCronStatus().finally(() => {
                        this._scanning = false;
                        this._bannerEl.classList.remove("fraud-cron-scanning");
                    });
                }, 1500);
            }
            return;
        }

        const mm = String(Math.floor(diff / 60)).padStart(2, "0");
        const ss = String(diff % 60).padStart(2, "0");
        valueEl.textContent = `${mm}:${ss}`;
    }

    _onBusNotification({ detail }) {
        const notifications = detail || [];
        let needsReload = false;
        for (const { type, payload } of notifications) {
            if (type === "new_alert") {
                needsReload = true;
                const who = payload.beneficiary_name || `Beneficiary #${payload.beneficiary_id}`;
                this.notification.add(
                    `${who} — score ${(payload.final_score * 100).toFixed(1)}%`,
                    {
                        title: `${payload.risk_level} alert`,
                        type: payload.risk_level === "CRITICAL" ? "danger" : "warning",
                        sticky: false,
                    },
                );
            }
        }
        if (needsReload && this.model) {
            this.model.load();
            this._refreshCronStatus();
        }
    }
}

export const fraudLiveMonitor = {
    ...kanbanView,
    Controller: FraudLiveMonitorController,
    Renderer: KanbanRenderer,
};

registry.category("views").add("fraud_live_monitor", fraudLiveMonitor);
