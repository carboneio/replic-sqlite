{
  "targets": [{
    "target_name": "keep_last",
    "type": "shared_library",
    "sources": [ "src/keep_last.c" ],
    "include_dirs": [
      "<!(node -p \"require('path').join(require('path').dirname(require.resolve('better-sqlite3/package.json')), 'deps', 'sqlite3')\")"
    ],
    'xcode_settings': {
      'MACOSX_DEPLOYMENT_TARGET': '10.15',
      'GCC_OPTIMIZATION_LEVEL': '3',
      'GCC_GENERATE_DEBUGGING_SYMBOLS': 'NO',
      'DEAD_CODE_STRIPPING': 'YES',
      'GCC_INLINES_ARE_PRIVATE_EXTERN': 'YES',
    },
    "defines": [],
    'cflags': ['-std=c99 -fPIC -O3'],
    'conditions': [
      ['OS == "win"', {
        'defines': ['WIN32'],
      }]
    ]
  }]
} 