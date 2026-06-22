import { create as createSubject } from '@liquid-bricks/lib-nats-subject/create/basic'

import { events as natsEvents } from '@liquid-bricks/lib-nats-subject/events/nats'


export const path = createSubject(natsEvents['*'].gateway['*'].function_result.evt.component.compute_function.v1['*'])
  .forSubscribe()
  .toObject()

export const spec = {
  handler: async ({ message, rootCtx: { natsContext } }) => {
    const { instanceId, result, type, name } = message?.data ?? {}
    switch (type) {
      case 'data': {
        const subject = createSubject(natsEvents['*'].component_service['*'].function_result.evt.component.compute_function.v1.data).forPublish()
          .env('prod')

        await natsContext.publish(
          subject.build(),
          JSON.stringify({ data: { instanceId, name, type, result } })
        )
        return
      }
      case 'gate': {
        const subject = createSubject(natsEvents['*'].component_service['*'].function_result.evt.component.compute_function.v1.gate).forPublish()
          .env('prod')

        await natsContext.publish(
          subject.build(),
          JSON.stringify({ data: { instanceId, name, type, result } })
        )
        return
      }
      case 'task': {
        const subject = createSubject(natsEvents['*'].component_service['*'].function_result.evt.component.compute_function.v1.task).forPublish()
          .env('prod')

        await natsContext.publish(
          subject.build(),
          JSON.stringify({ data: { instanceId, name, type, result } })
        )
        return
      }
      default:
        throw new TypeError('Unsupported compute_function result type: ' + type)
    }
  },
}
