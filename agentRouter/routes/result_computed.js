import { create as createSubject } from '@liquid-bricks/shared-providers/subject/create/basic'

export const path = {
  context: 'component-agent',
  channel: 'evt',
  entity: 'component',
  action: 'result_computed',
}

export const spec = {
  handler: async ({ message, rootCtx: { natsContext } }) => {
    const { instanceId, result, type, name } = message?.data ?? {}

    const subject = createSubject()
      .env('prod')
      .ns('component-service')
      .entity('componentInstance')
      .channel('evt')
      .action('result_computed')
      .version('v1')

    await natsContext.publish(
      subject.build(),
      JSON.stringify({ data: { instanceId, name, type, result } })
    )
  },
}
