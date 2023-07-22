export interface StateMachine {
  apply(command: any): Promise<any>;
}
