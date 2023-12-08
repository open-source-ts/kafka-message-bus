'use strict';

module.exports = {

  env: {
    node: true,
    jest: true
  },

  root: true,
  parser: '@typescript-eslint/parser',
  plugins: [
    '@typescript-eslint',
    'jest'
  ],
  extends: [
    'eslint:recommended',
    'plugin:@typescript-eslint/recommended',
    'plugin:jest/recommended'
  ],
  rules: {
    "@typescript-eslint/no-explicit-any": "off",
    '@typescript-eslint/ban-types': ['error',
      {
        'types': {
          'Object': false,
          '{}': false
        },
        'extendDefaults': true
      }
    ]
  }
};
