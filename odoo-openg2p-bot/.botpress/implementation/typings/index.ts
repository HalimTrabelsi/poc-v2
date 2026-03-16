/* eslint-disable */
/* tslint:disable */
// This file is generated. Do not edit it manually.

import * as integrations from './integrations/index'
import * as events from './events'
import * as states from './states'
import * as actions from './actions'
import * as tables from './tables/index'
import * as workflows from './workflows'

export * as integrations from './integrations/index'
export * as events from './events/index'
export * as states from './states/index'
export * as actions from './actions'
export * as tables from './tables/index'
export * as workflows from './workflows'

export type TBot = {
  integrations: integrations.Integrations
  events: events.Events
  states: states.States
  actions: actions.Actions
  tables: tables.Tables
  workflows: workflows.Workflows
}