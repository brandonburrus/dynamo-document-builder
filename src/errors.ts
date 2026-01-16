/**
 * Custom error class for handling Document Builder related errors.
 */
export class DocumentBuilderError extends Error {
  constructor(message: string) {
    super(`DocumentBuilderError: ${message}`)
    this.name = 'DocumentBuilderError'
    this.message = message
  }
}
