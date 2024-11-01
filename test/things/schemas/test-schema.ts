import { Column } from '../../../src/decorators/column/column'
import { Schema } from '../../../src/decorators/schema/schema'

@Schema({ engine: 'MergeTree' })
export class TestSchema {
  @Column({ type: 'UUID' })
  id: string

  @Column({ type: 'String' })
  name: string

  @Column({ type: 'DateTime' })
  dateOfBirth: string

  @Column({ type: 'Array(Int8)' })
  numericArray: number[]

  @Column({ type: 'Tuple(String, String)' })
  s: any
}
