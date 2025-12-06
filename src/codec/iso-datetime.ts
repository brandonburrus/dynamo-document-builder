import { codec, date, iso } from 'zod/v4'

export const isoDatetime = () =>
  codec(iso.datetime(), date(), {
    decode: (isoString: string) => new Date(isoString),
    encode: (date: Date) => date.toISOString(),
  })
