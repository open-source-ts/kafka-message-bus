const jestConsole = console;

beforeAll(() => {
    global.console = require('console');
});

afterAll(() => {
    global.console = jestConsole;
});
