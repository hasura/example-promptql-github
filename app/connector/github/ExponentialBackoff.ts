export interface BackoffOptions {
  initialDelay: number; // Initial delay in milliseconds
  maxDelay?: number; // Maximum delay in milliseconds
  maxAttempts?: number; // Maximum number of retry attempts
  factor?: number; // Multiplication factor for each attempt
  jitter?: boolean; // Add randomization to delays
}

export class ExponentialBackoff {
  private currentAttempt: number = 0;
  private readonly options: Required<BackoffOptions>;

  constructor(options: BackoffOptions) {
    this.options = {
      initialDelay: options.initialDelay,
      maxDelay: options.maxDelay ?? 30000,
      maxAttempts: options.maxAttempts ?? 5,
      factor: options.factor ?? 2,
      jitter: options.jitter ?? true,
    };
  }

  async execute<T>(operation: () => Promise<T>): Promise<T> {
    while (true) {
      try {
        return await operation();
      } catch (error) {
        if (
          this.options.maxAttempts > 0 &&
          this.currentAttempt >= this.options.maxAttempts - 1
        ) {
          throw error;
        }

        console.log("ERROR: ", error);
        console.log("Retrying in ", this.calculateDelay() / 1000, "seconds...");

        await this.wait();
        this.currentAttempt++;
      }
    }
  }

  private async wait(): Promise<void> {
    const delay = this.calculateDelay();
    await new Promise((resolve) => setTimeout(resolve, delay));
  }

  private calculateDelay(): number {
    const exponentialDelay =
      this.options.initialDelay *
      Math.pow(this.options.factor, this.currentAttempt);

    const boundedDelay = Math.min(exponentialDelay, this.options.maxDelay);

    if (!this.options.jitter) {
      return boundedDelay;
    }

    // Add random jitter between 0% and 25% of the delay
    const jitterAmount = boundedDelay * 0.25 * Math.random();
    return boundedDelay + jitterAmount;
  }

  reset(): void {
    this.currentAttempt = 0;
  }
}
