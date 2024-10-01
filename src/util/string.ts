export function snakeCase(name: string): string {
  return name.replace(/([a-z])([A-Z])/g, '$1_$2').toLowerCase()
}
