import { Column } from '../../src/decorators/column/column'
import { Schema } from '../../src/decorators/schema/schema'

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
