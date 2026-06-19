import { create as createSubject } from '@liquid-bricks/lib-nats-subject/create/basic'

import { events as natsEvents } from '@liquid-bricks/lib-nats-subject/events/nats'


export const path = createSubject(natsEvents['*'].gateway['*'].function_result.evt.component.compute_function.v1['*'])
  .forSubscribe()
  .toObject()

export const spec = {
  handler: async ({ message, rootCtx: { natsContext } }) => {
    const { instanceId, result, type, name } = message?.data ?? {}

    const subject = createSubject(natsEvents['*'].component_service['*'].function_result.evt.component.compute_function.v1['*']).forPublish()
      .env('prod')

    await natsContext.publish(
      subject.build(),
      JSON.stringify({ data: { instanceId, name, type, result } })
    )
  },
}
