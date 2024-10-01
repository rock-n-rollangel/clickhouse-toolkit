import { Column } from '@/decorators/column/column'
import { Schema } from '@/decorators/schema/schema'

@Schema({
  engine: 'MergeTree',
})
export class TestSchema {
  @Column({ type: 'UUID' })
  id: string

  @Column({ type: 'String' })
  name: string

  @Column({ type: 'Int8' })
  age: number

  @Column({ type: 'DateTime' })
  dateOfBirth: string
}
