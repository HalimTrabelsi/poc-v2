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

        // bus_service is async — useService can't wrap it (Odoo 17 quirk).
        // Reach it directly through env.services instead.
        this.busService = this.env.services.bus_service;
        this.notification = useService("notification");

        this._busHandler = this._onBusNotification.bind(this);

        onMounted(() => {
            if (!this.busService) {
                return;
            }
            this.busService.addChannel("fraud_alerts");
            this.busService.addEventListener("notification", this._busHandler);
        });

        onWillUnmount(() => {
            if (!this.busService) {
                return;
            }
            this.busService.removeEventListener("notification", this._busHandler);
            this.busService.deleteChannel("fraud_alerts");
        });
    }

    _onBusNotification({ detail }) {
        const notifications = detail || [];
        let needsReload = false;
        for (const { type, payload } of notifications) {
            if (type === "new_alert") {
                needsReload = true;
                this.notification.add(
                    `Beneficiary #${payload.beneficiary_id} — score ${(payload.final_score * 100).toFixed(1)}%`,
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
        }
    }
}

export const fraudLiveMonitor = {
    ...kanbanView,
    Controller: FraudLiveMonitorController,
    Renderer: KanbanRenderer,
};

registry.category("views").add("fraud_live_monitor", fraudLiveMonitor);
