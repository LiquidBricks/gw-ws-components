import { create as createSubject } from '@liquid-bricks/lib-nats-subject/create/basic'

import { events as natsEvents } from '@liquid-bricks/lib-nats-subject/events/nats'


export const path = {
  context: 'component-agent',
  channel: 'evt',
  entity: 'component',
  action: 'computeResultDone',
}

export const spec = {
  handler: async ({ message, rootCtx: { natsContext } }) => {
    const { instanceId, result, type, name } = message?.data ?? {}

    const subject = createSubject(natsEvents['*'].component_service['*']['*'].evt.componentInstance.computeResultDone.v1['*']).forPublish()
      .env('prod')

    await natsContext.publish(
      subject.build(),
      JSON.stringify({ data: { instanceId, name, type, result } })
    )
  },
}
