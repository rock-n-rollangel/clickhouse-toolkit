import { Column } from '@/decorators/column/column'
import { Schema } from '@/decorators/schema/schema'

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
}
