export class StateMachine {
  public async apply(command: any): Promise<any> {
    return {
      hello: 'world',
    };
  }
}
