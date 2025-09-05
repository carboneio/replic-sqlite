const globals = require('globals');
const js = require('@eslint/js');

const {
  FlatCompat,
} = require('@eslint/eslintrc');

const compat = new FlatCompat({
  baseDirectory     : __dirname,
  recommendedConfig : js.configs.recommended,
  allConfig         : js.configs.all
});

module.exports = [...compat.extends('eslint:recommended'), {
  plugins : {},

  languageOptions : {
    globals : {
      ...Object.fromEntries(Object.entries(globals.browser).map(([key]) => [key, 'off'])),
      ...globals.node,
      ...globals.mocha,
    },

    ecmaVersion : 2020,
    sourceType  : 'commonjs',
  },

  rules : {
    // 'one-var'                     : ['warn', 'never'],       // Disallow declaration of multiple var in one line
    'space-before-blocks'         : ['warn', 'always'],      // Enforce space before {}
    'space-before-function-paren' : ['warn', 'always'],      // Enforce space before parenthesis of named function
    'spaced-comment'              : ['warn', 'always'],      // Enforce space at the start of comments
    'linebreak-style'             : ['warn', 'unix'],        // Use \n as breakline character
    'quote-props'                 : ['warn', 'consistent', {unnecessary : false} ],   // Only use quotes around properties if key is not ES valid
    'brace-style'                 : ['warn', 'stroustrup'],  // Enforce opening bracket at the end of the line
    'semi'                        : ['error', 'always'],     // Enforce semi-colon ath end of statements
    'eqeqeq'                      : ['error', 'always'],     // Enforce usage of === instead of ==
    'dot-location'                : ['error', 'property'],   // Enforce the dot be linked to the property instead of the parent
    'curly'                       : ['error', 'all'],        // Enforce usage of brackets everywherewhich eslint
    'quotes'                      : ['error', 'single', {    // Enforce usage of single quotes instead of double
      avoidEscape : true,
    }],
    //'no-cond-assign'                : ['error', 'always'],  // Disallow assignment in condition
    'no-dupe-args'                  : 'error',              // Disallow duplicated arguments in function
    'no-dupe-keys'                  : 'error',              // Disallow duplicated keys in object
    'no-duplicate-case'             : 'error',              // Disallow duplicated case in switch
    'no-empty'                      : 'error',              // Disallow empty blocks
    'no-ex-assign'                  : 'error',              // Disallow reassigning exceptions in catch
    'no-func-assign'                : 'error',              // Disallow reassigning function
    'no-trailing-spaces'            : 'error',
    'no-unused-vars'                : 'error',              // Disallow unused variables
    'no-unreachable'                : 'error',              // Disallow unreachable code
    'array-callback-return'         : 'error',              // Enforce return statements in array functions
    'dot-notation'                  : 'error',              // Enforce dot notation over square brackets
    'block-scoped-var'              : 'error',              // Treat var as Block Scoped (block-scoped-var), why not using "let"? because "let" is 10 times slower than "var"
    'no-sparse-arrays'              : 'warn',               // Enforce empty slots at the end of array declaration
    'no-irregular-whitespace'       : 'warn',               // Enforce usage of whitespace
    // camelcase                       : 'warn',               // Enforce usage of CamelCase over snake_case
    'no-whitespace-before-property' : 'warn',               // Disallow whitespace before property

    'indent' : ['warn', 2, {                                  // Indent with 2 spaces
      SwitchCase       : 1,
      MemberExpression : 1,
    }],

    'no-console' : ['error', {                              // Disallow console usage for other than warn, error or log
      allow : ['warn', 'error', 'log'],
    }],

    'keyword-spacing' : ['warn', {                          // Enforce whitespace before and after keywords
      before : true,
      after  : true,
    }],

    'max-statements-per-line' : ['warn', {                  // Disallow multiple statements per line
      max : 1,
    }],

    'key-spacing' : ['warn', {                              // Enforce whitespace before and after colon in object
      beforeColon : true,
      afterColon  : true,
      align       : 'colon',
      mode        : 'strict',
    }],
  },
}];