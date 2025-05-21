import * as path from 'path'

export function pathNormalize(pathStr: string): string {
  let normalizedPath = path.normalize(pathStr)
  if (process.platform === 'win32') normalizedPath = normalizedPath.replace(/\\/g, '/')
  return normalizedPath
}

export function pathExtname(pathStr: string): string {
  return path.extname(pathStr)
}

export function pathResolve(pathStr: string): string {
  return path.resolve(pathStr)
}
