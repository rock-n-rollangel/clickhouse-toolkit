import { ObjectLiteral } from './object-literal'

export type Params<T extends ObjectLiteral> =
  | {
      [key in keyof T]: T[key]
    }
  | {
      [key: string]: any
    }
