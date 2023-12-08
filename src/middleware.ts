type Next = () => Promise<void> | void;

export type Middleware<T> = (context: T, next: Next) => Promise<void> | void;

export type Pipeline<T> = {
    push: (...middlewares: Middleware<T>[]) => void;
    execute: (context: T) => Promise<void>;
};

export function Pipeline<T>(...middlewares: Middleware<T>[]): Pipeline<T> {
    const stack: Middleware<T>[] = middlewares;

    const push: Pipeline<T>['push'] = (...middlewares) => {
        stack.push(...middlewares);
    };

    const execute: Pipeline<T>['execute'] = async (context) => {
        let prevIndex = -1;

        const runner = async (index: number): Promise<void> => {
            if (index === prevIndex) {
                throw new Error('next() called multiple times');
            }

            prevIndex = index;

            const middleware = stack[index];

            if (middleware) {
                await middleware(context, () => {
                    return runner(index + 1);
                });
            }
        };

        await runner(0);
    };

    return { push, execute };
}
