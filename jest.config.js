module.exports = {
    preset: 'ts-jest',
    testEnvironment: 'node',
    roots: [
        "<rootDir>/tests"
    ],
    setupFilesAfterEnv: ['<rootDir>/tests/setup.ts'],
    transform: {
        "^.+\\.tsx?$": "ts-jest"
    },
    testRegex: "tests/.*\\.test\\.(js|ts)$",
    moduleFileExtensions: [
        "ts",
        "tsx",
        "json",
        'js',
        'jsx'
    ],
    collectCoverage: true,
    collectCoverageFrom: ["<rootDir>/src/**/*.ts"],
    coverageDirectory: "./coverage",
    coverageThreshold: {
        global: {
            branches: 64,
            functions: 90,
            lines: 83,
            statements: 84
        }
    }
};
