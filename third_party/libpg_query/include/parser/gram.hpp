/* A Bison parser, made by GNU Bison 2.3.  */

/* Skeleton interface for Bison's Yacc-like parsers in C

   Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor,
   Boston, MA 02110-1301, USA.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     IDENT = 258,
     FCONST = 259,
     SCONST = 260,
     BCONST = 261,
     XCONST = 262,
     Op = 263,
     ICONST = 264,
     PARAM = 265,
     TYPECAST = 266,
     DOT_DOT = 267,
     COLON_EQUALS = 268,
     EQUALS_GREATER = 269,
     POWER_OF = 270,
     LAMBDA_ARROW = 271,
     DOUBLE_ARROW = 272,
     LESS_EQUALS = 273,
     GREATER_EQUALS = 274,
     NOT_EQUALS = 275,
     ABORT_P = 276,
     ABSOLUTE_P = 277,
     ACCESS = 278,
     ACTION = 279,
     ADD_P = 280,
     ADMIN = 281,
     AFTER = 282,
     AGGREGATE = 283,
     ALL = 284,
     ALSO = 285,
     ALTER = 286,
     ALWAYS = 287,
     ANALYSE = 288,
     ANALYZE = 289,
     AND = 290,
     ANTI = 291,
     ANY = 292,
     ARRAY = 293,
     AS = 294,
     ASC_P = 295,
     ASSERTION = 296,
     ASSIGNMENT = 297,
     ASYMMETRIC = 298,
     AT = 299,
     ATTACH = 300,
     ATTRIBUTE = 301,
     AUTHORIZATION = 302,
     BACKWARD = 303,
     BEFORE = 304,
     BEGIN_P = 305,
     BETWEEN = 306,
     BIGINT = 307,
     BINARY = 308,
     BIT = 309,
     BOOLEAN_P = 310,
     BOTH = 311,
     BY = 312,
     CACHE = 313,
     CALL_P = 314,
     CALLED = 315,
     CASCADE = 316,
     CASCADED = 317,
     CASE = 318,
     CAST = 319,
     CATALOG_P = 320,
     CHAIN = 321,
     CHAR_P = 322,
     CHARACTER = 323,
     CHARACTERISTICS = 324,
     CHECK_P = 325,
     CHECKPOINT = 326,
     CLASS = 327,
     CLOSE = 328,
     CLUSTER = 329,
     COALESCE = 330,
     COLLATE = 331,
     COLLATION = 332,
     COLUMN = 333,
     COLUMNS = 334,
     COMMENT = 335,
     COMMENTS = 336,
     COMMIT = 337,
     COMMITTED = 338,
     COMPRESSION = 339,
     CONCURRENTLY = 340,
     CONFIGURATION = 341,
     CONFLICT = 342,
     CONNECTION = 343,
     CONSTRAINT = 344,
     CONSTRAINTS = 345,
     CONTENT_P = 346,
     CONTINUE_P = 347,
     CONVERSION_P = 348,
     COPY = 349,
     COST = 350,
     CREATE_P = 351,
     CROSS = 352,
     CSV = 353,
     CUBE = 354,
     CURRENT_P = 355,
     CURRENT_CATALOG = 356,
     CURRENT_DATE = 357,
     CURRENT_ROLE = 358,
     CURRENT_SCHEMA = 359,
     CURRENT_TIME = 360,
     CURRENT_TIMESTAMP = 361,
     CURRENT_USER = 362,
     CURSOR = 363,
     CYCLE = 364,
     DATA_P = 365,
     DATABASE = 366,
     DAY_P = 367,
     DAYS_P = 368,
     DEALLOCATE = 369,
     DEC = 370,
     DECIMAL_P = 371,
     DECLARE = 372,
     DEFAULT = 373,
     DEFAULTS = 374,
     DEFERRABLE = 375,
     DEFERRED = 376,
     DEFINER = 377,
     DELETE_P = 378,
     DELIMITER = 379,
     DELIMITERS = 380,
     DEPENDS = 381,
     DESC_P = 382,
     DESCRIBE = 383,
     DETACH = 384,
     DICTIONARY = 385,
     DISABLE_P = 386,
     DISCARD = 387,
     DISTINCT = 388,
     DO = 389,
     DOCUMENT_P = 390,
     DOMAIN_P = 391,
     DOUBLE_P = 392,
     DROP = 393,
     EACH = 394,
     ELSE = 395,
     ENABLE_P = 396,
     ENCODING = 397,
     ENCRYPTED = 398,
     END_P = 399,
     ENUM_P = 400,
     ESCAPE = 401,
     EVENT = 402,
     EXCEPT = 403,
     EXCLUDE = 404,
     EXCLUDING = 405,
     EXCLUSIVE = 406,
     EXECUTE = 407,
     EXISTS = 408,
     EXPLAIN = 409,
     EXPORT_P = 410,
     EXPORT_STATE = 411,
     EXTENSION = 412,
     EXTERNAL = 413,
     EXTRACT = 414,
     FALSE_P = 415,
     FAMILY = 416,
     FETCH = 417,
     FILTER = 418,
     FIRST_P = 419,
     FLOAT_P = 420,
     FOLLOWING = 421,
     FOR = 422,
     FORCE = 423,
     FOREIGN = 424,
     FORWARD = 425,
     FREEZE = 426,
     FROM = 427,
     FULL = 428,
     FUNCTION = 429,
     FUNCTIONS = 430,
     GENERATED = 431,
     GLOB = 432,
     GLOBAL = 433,
     GRANT = 434,
     GRANTED = 435,
     GROUP_P = 436,
     GROUPING = 437,
     GROUPING_ID = 438,
     HANDLER = 439,
     HAVING = 440,
     HEADER_P = 441,
     HOLD = 442,
     HOUR_P = 443,
     HOURS_P = 444,
     IDENTITY_P = 445,
     IF_P = 446,
     IGNORE_P = 447,
     ILIKE = 448,
     IMMEDIATE = 449,
     IMMUTABLE = 450,
     IMPLICIT_P = 451,
     IMPORT_P = 452,
     IN_P = 453,
     INCLUDING = 454,
     INCREMENT = 455,
     INDEX = 456,
     INDEXES = 457,
     INHERIT = 458,
     INHERITS = 459,
     INITIALLY = 460,
     INLINE_P = 461,
     INNER_P = 462,
     INOUT = 463,
     INPUT_P = 464,
     INSENSITIVE = 465,
     INSERT = 466,
     INSTALL = 467,
     INSTEAD = 468,
     INT_P = 469,
     INTEGER = 470,
     INTERSECT = 471,
     INTERVAL = 472,
     INTO = 473,
     INVOKER = 474,
     IS = 475,
     ISNULL = 476,
     ISOLATION = 477,
     JOIN = 478,
     JSON = 479,
     KEY = 480,
     LABEL = 481,
     LANGUAGE = 482,
     LARGE_P = 483,
     LAST_P = 484,
     LATERAL_P = 485,
     LEADING = 486,
     LEAKPROOF = 487,
     LEFT = 488,
     LEVEL = 489,
     LIKE = 490,
     LIMIT = 491,
     LISTEN = 492,
     LOAD = 493,
     LOCAL = 494,
     LOCALTIME = 495,
     LOCALTIMESTAMP = 496,
     LOCATION = 497,
     LOCK_P = 498,
     LOCKED = 499,
     LOGGED = 500,
     MACRO = 501,
     MAP = 502,
     MAPPING = 503,
     MATCH = 504,
     MATERIALIZED = 505,
     MAXVALUE = 506,
     METHOD = 507,
     MICROSECOND_P = 508,
     MICROSECONDS_P = 509,
     MILLISECOND_P = 510,
     MILLISECONDS_P = 511,
     MINUTE_P = 512,
     MINUTES_P = 513,
     MINVALUE = 514,
     MODE = 515,
     MONTH_P = 516,
     MONTHS_P = 517,
     MOVE = 518,
     NAME_P = 519,
     NAMES = 520,
     NATIONAL = 521,
     NATURAL = 522,
     NCHAR = 523,
     NEW = 524,
     NEXT = 525,
     NO = 526,
     NONE = 527,
     NOT = 528,
     NOTHING = 529,
     NOTIFY = 530,
     NOTNULL = 531,
     NOWAIT = 532,
     NULL_P = 533,
     NULLIF = 534,
     NULLS_P = 535,
     NUMERIC = 536,
     OBJECT_P = 537,
     OF = 538,
     OFF = 539,
     OFFSET = 540,
     OIDS = 541,
     OLD = 542,
     ON = 543,
     ONLY = 544,
     OPERATOR = 545,
     OPTION = 546,
     OPTIONS = 547,
     OR = 548,
     ORDER = 549,
     ORDINALITY = 550,
     OUT_P = 551,
     OUTER_P = 552,
     OVER = 553,
     OVERLAPS = 554,
     OVERLAY = 555,
     OVERRIDING = 556,
     OWNED = 557,
     OWNER = 558,
     PARALLEL = 559,
     PARSER = 560,
     PARTIAL = 561,
     PARTITION = 562,
     PASSING = 563,
     PASSWORD = 564,
     PERCENT = 565,
     PLACING = 566,
     PLANS = 567,
     POLICY = 568,
     POSITION = 569,
     POSITIONAL = 570,
     PRAGMA_P = 571,
     PRECEDING = 572,
     PRECISION = 573,
     PREPARE = 574,
     PREPARED = 575,
     PRESERVE = 576,
     PRIMARY = 577,
     PRIOR = 578,
     PRIVILEGES = 579,
     PROCEDURAL = 580,
     PROCEDURE = 581,
     PROGRAM = 582,
     PUBLICATION = 583,
     QUALIFY = 584,
     QUOTE = 585,
     RANGE = 586,
     READ_P = 587,
     REAL = 588,
     REASSIGN = 589,
     RECHECK = 590,
     RECURSIVE = 591,
     REF = 592,
     REFERENCES = 593,
     REFERENCING = 594,
     REFRESH = 595,
     REINDEX = 596,
     RELATIVE_P = 597,
     RELEASE = 598,
     RENAME = 599,
     REPEATABLE = 600,
     REPLACE = 601,
     REPLICA = 602,
     RESET = 603,
     RESPECT_P = 604,
     RESTART = 605,
     RESTRICT = 606,
     RETURNING = 607,
     RETURNS = 608,
     REVOKE = 609,
     RIGHT = 610,
     ROLE = 611,
     ROLLBACK = 612,
     ROLLUP = 613,
     ROW = 614,
     ROWS = 615,
     RULE = 616,
     SAMPLE = 617,
     SAVEPOINT = 618,
     SCHEMA = 619,
     SCHEMAS = 620,
     SCROLL = 621,
     SEARCH = 622,
     SECOND_P = 623,
     SECONDS_P = 624,
     SECURITY = 625,
     SELECT = 626,
     SEMI = 627,
     SEQUENCE = 628,
     SEQUENCES = 629,
     SERIALIZABLE = 630,
     SERVER = 631,
     SESSION = 632,
     SESSION_USER = 633,
     SET = 634,
     SETOF = 635,
     SETS = 636,
     SHARE = 637,
     SHOW = 638,
     SIMILAR = 639,
     SIMPLE = 640,
     SKIP = 641,
     SMALLINT = 642,
     SNAPSHOT = 643,
     SOME = 644,
     SQL_P = 645,
     STABLE = 646,
     STANDALONE_P = 647,
     START = 648,
     STATEMENT = 649,
     STATISTICS = 650,
     STDIN = 651,
     STDOUT = 652,
     STORAGE = 653,
     STORED = 654,
     STRICT_P = 655,
     STRIP_P = 656,
     STRUCT = 657,
     SUBSCRIPTION = 658,
     SUBSTRING = 659,
     SUMMARIZE = 660,
     SYMMETRIC = 661,
     SYSID = 662,
     SYSTEM_P = 663,
     TABLE = 664,
     TABLES = 665,
     TABLESAMPLE = 666,
     TABLESPACE = 667,
     TEMP = 668,
     TEMPLATE = 669,
     TEMPORARY = 670,
     TEXT_P = 671,
     THEN = 672,
     TIME = 673,
     TIMESTAMP = 674,
     TO = 675,
     TRAILING = 676,
     TRANSACTION = 677,
     TRANSFORM = 678,
     TREAT = 679,
     TRIGGER = 680,
     TRIM = 681,
     TRUE_P = 682,
     TRUNCATE = 683,
     TRUSTED = 684,
     TRY_CAST = 685,
     TYPE_P = 686,
     TYPES_P = 687,
     UNBOUNDED = 688,
     UNCOMMITTED = 689,
     UNENCRYPTED = 690,
     UNION = 691,
     UNIQUE = 692,
     UNKNOWN = 693,
     UNLISTEN = 694,
     UNLOGGED = 695,
     UNTIL = 696,
     UPDATE = 697,
     USE_P = 698,
     USER = 699,
     USING = 700,
     VACUUM = 701,
     VALID = 702,
     VALIDATE = 703,
     VALIDATOR = 704,
     VALUE_P = 705,
     VALUES = 706,
     VARCHAR = 707,
     VARIADIC = 708,
     VARYING = 709,
     VERBOSE = 710,
     VERSION_P = 711,
     VIEW = 712,
     VIEWS = 713,
     VIRTUAL = 714,
     VOLATILE = 715,
     WHEN = 716,
     WHERE = 717,
     WHITESPACE_P = 718,
     WINDOW = 719,
     WITH = 720,
     WITHIN = 721,
     WITHOUT = 722,
     WORK = 723,
     WRAPPER = 724,
     WRITE_P = 725,
     XML_P = 726,
     XMLATTRIBUTES = 727,
     XMLCONCAT = 728,
     XMLELEMENT = 729,
     XMLEXISTS = 730,
     XMLFOREST = 731,
     XMLNAMESPACES = 732,
     XMLPARSE = 733,
     XMLPI = 734,
     XMLROOT = 735,
     XMLSERIALIZE = 736,
     XMLTABLE = 737,
     YEAR_P = 738,
     YEARS_P = 739,
     YES_P = 740,
     ZONE = 741,
     NOT_LA = 742,
     NULLS_LA = 743,
     WITH_LA = 744,
     POSTFIXOP = 745,
     UMINUS = 746
   };
#endif
/* Tokens.  */
#define IDENT 258
#define FCONST 259
#define SCONST 260
#define BCONST 261
#define XCONST 262
#define Op 263
#define ICONST 264
#define PARAM 265
#define TYPECAST 266
#define DOT_DOT 267
#define COLON_EQUALS 268
#define EQUALS_GREATER 269
#define POWER_OF 270
#define LAMBDA_ARROW 271
#define DOUBLE_ARROW 272
#define LESS_EQUALS 273
#define GREATER_EQUALS 274
#define NOT_EQUALS 275
#define ABORT_P 276
#define ABSOLUTE_P 277
#define ACCESS 278
#define ACTION 279
#define ADD_P 280
#define ADMIN 281
#define AFTER 282
#define AGGREGATE 283
#define ALL 284
#define ALSO 285
#define ALTER 286
#define ALWAYS 287
#define ANALYSE 288
#define ANALYZE 289
#define AND 290
#define ANTI 291
#define ANY 292
#define ARRAY 293
#define AS 294
#define ASC_P 295
#define ASSERTION 296
#define ASSIGNMENT 297
#define ASYMMETRIC 298
#define AT 299
#define ATTACH 300
#define ATTRIBUTE 301
#define AUTHORIZATION 302
#define BACKWARD 303
#define BEFORE 304
#define BEGIN_P 305
#define BETWEEN 306
#define BIGINT 307
#define BINARY 308
#define BIT 309
#define BOOLEAN_P 310
#define BOTH 311
#define BY 312
#define CACHE 313
#define CALL_P 314
#define CALLED 315
#define CASCADE 316
#define CASCADED 317
#define CASE 318
#define CAST 319
#define CATALOG_P 320
#define CHAIN 321
#define CHAR_P 322
#define CHARACTER 323
#define CHARACTERISTICS 324
#define CHECK_P 325
#define CHECKPOINT 326
#define CLASS 327
#define CLOSE 328
#define CLUSTER 329
#define COALESCE 330
#define COLLATE 331
#define COLLATION 332
#define COLUMN 333
#define COLUMNS 334
#define COMMENT 335
#define COMMENTS 336
#define COMMIT 337
#define COMMITTED 338
#define COMPRESSION 339
#define CONCURRENTLY 340
#define CONFIGURATION 341
#define CONFLICT 342
#define CONNECTION 343
#define CONSTRAINT 344
#define CONSTRAINTS 345
#define CONTENT_P 346
#define CONTINUE_P 347
#define CONVERSION_P 348
#define COPY 349
#define COST 350
#define CREATE_P 351
#define CROSS 352
#define CSV 353
#define CUBE 354
#define CURRENT_P 355
#define CURRENT_CATALOG 356
#define CURRENT_DATE 357
#define CURRENT_ROLE 358
#define CURRENT_SCHEMA 359
#define CURRENT_TIME 360
#define CURRENT_TIMESTAMP 361
#define CURRENT_USER 362
#define CURSOR 363
#define CYCLE 364
#define DATA_P 365
#define DATABASE 366
#define DAY_P 367
#define DAYS_P 368
#define DEALLOCATE 369
#define DEC 370
#define DECIMAL_P 371
#define DECLARE 372
#define DEFAULT 373
#define DEFAULTS 374
#define DEFERRABLE 375
#define DEFERRED 376
#define DEFINER 377
#define DELETE_P 378
#define DELIMITER 379
#define DELIMITERS 380
#define DEPENDS 381
#define DESC_P 382
#define DESCRIBE 383
#define DETACH 384
#define DICTIONARY 385
#define DISABLE_P 386
#define DISCARD 387
#define DISTINCT 388
#define DO 389
#define DOCUMENT_P 390
#define DOMAIN_P 391
#define DOUBLE_P 392
#define DROP 393
#define EACH 394
#define ELSE 395
#define ENABLE_P 396
#define ENCODING 397
#define ENCRYPTED 398
#define END_P 399
#define ENUM_P 400
#define ESCAPE 401
#define EVENT 402
#define EXCEPT 403
#define EXCLUDE 404
#define EXCLUDING 405
#define EXCLUSIVE 406
#define EXECUTE 407
#define EXISTS 408
#define EXPLAIN 409
#define EXPORT_P 410
#define EXPORT_STATE 411
#define EXTENSION 412
#define EXTERNAL 413
#define EXTRACT 414
#define FALSE_P 415
#define FAMILY 416
#define FETCH 417
#define FILTER 418
#define FIRST_P 419
#define FLOAT_P 420
#define FOLLOWING 421
#define FOR 422
#define FORCE 423
#define FOREIGN 424
#define FORWARD 425
#define FREEZE 426
#define FROM 427
#define FULL 428
#define FUNCTION 429
#define FUNCTIONS 430
#define GENERATED 431
#define GLOB 432
#define GLOBAL 433
#define GRANT 434
#define GRANTED 435
#define GROUP_P 436
#define GROUPING 437
#define GROUPING_ID 438
#define HANDLER 439
#define HAVING 440
#define HEADER_P 441
#define HOLD 442
#define HOUR_P 443
#define HOURS_P 444
#define IDENTITY_P 445
#define IF_P 446
#define IGNORE_P 447
#define ILIKE 448
#define IMMEDIATE 449
#define IMMUTABLE 450
#define IMPLICIT_P 451
#define IMPORT_P 452
#define IN_P 453
#define INCLUDING 454
#define INCREMENT 455
#define INDEX 456
#define INDEXES 457
#define INHERIT 458
#define INHERITS 459
#define INITIALLY 460
#define INLINE_P 461
#define INNER_P 462
#define INOUT 463
#define INPUT_P 464
#define INSENSITIVE 465
#define INSERT 466
#define INSTALL 467
#define INSTEAD 468
#define INT_P 469
#define INTEGER 470
#define INTERSECT 471
#define INTERVAL 472
#define INTO 473
#define INVOKER 474
#define IS 475
#define ISNULL 476
#define ISOLATION 477
#define JOIN 478
#define JSON 479
#define KEY 480
#define LABEL 481
#define LANGUAGE 482
#define LARGE_P 483
#define LAST_P 484
#define LATERAL_P 485
#define LEADING 486
#define LEAKPROOF 487
#define LEFT 488
#define LEVEL 489
#define LIKE 490
#define LIMIT 491
#define LISTEN 492
#define LOAD 493
#define LOCAL 494
#define LOCALTIME 495
#define LOCALTIMESTAMP 496
#define LOCATION 497
#define LOCK_P 498
#define LOCKED 499
#define LOGGED 500
#define MACRO 501
#define MAP 502
#define MAPPING 503
#define MATCH 504
#define MATERIALIZED 505
#define MAXVALUE 506
#define METHOD 507
#define MICROSECOND_P 508
#define MICROSECONDS_P 509
#define MILLISECOND_P 510
#define MILLISECONDS_P 511
#define MINUTE_P 512
#define MINUTES_P 513
#define MINVALUE 514
#define MODE 515
#define MONTH_P 516
#define MONTHS_P 517
#define MOVE 518
#define NAME_P 519
#define NAMES 520
#define NATIONAL 521
#define NATURAL 522
#define NCHAR 523
#define NEW 524
#define NEXT 525
#define NO 526
#define NONE 527
#define NOT 528
#define NOTHING 529
#define NOTIFY 530
#define NOTNULL 531
#define NOWAIT 532
#define NULL_P 533
#define NULLIF 534
#define NULLS_P 535
#define NUMERIC 536
#define OBJECT_P 537
#define OF 538
#define OFF 539
#define OFFSET 540
#define OIDS 541
#define OLD 542
#define ON 543
#define ONLY 544
#define OPERATOR 545
#define OPTION 546
#define OPTIONS 547
#define OR 548
#define ORDER 549
#define ORDINALITY 550
#define OUT_P 551
#define OUTER_P 552
#define OVER 553
#define OVERLAPS 554
#define OVERLAY 555
#define OVERRIDING 556
#define OWNED 557
#define OWNER 558
#define PARALLEL 559
#define PARSER 560
#define PARTIAL 561
#define PARTITION 562
#define PASSING 563
#define PASSWORD 564
#define PERCENT 565
#define PLACING 566
#define PLANS 567
#define POLICY 568
#define POSITION 569
#define POSITIONAL 570
#define PRAGMA_P 571
#define PRECEDING 572
#define PRECISION 573
#define PREPARE 574
#define PREPARED 575
#define PRESERVE 576
#define PRIMARY 577
#define PRIOR 578
#define PRIVILEGES 579
#define PROCEDURAL 580
#define PROCEDURE 581
#define PROGRAM 582
#define PUBLICATION 583
#define QUALIFY 584
#define QUOTE 585
#define RANGE 586
#define READ_P 587
#define REAL 588
#define REASSIGN 589
#define RECHECK 590
#define RECURSIVE 591
#define REF 592
#define REFERENCES 593
#define REFERENCING 594
#define REFRESH 595
#define REINDEX 596
#define RELATIVE_P 597
#define RELEASE 598
#define RENAME 599
#define REPEATABLE 600
#define REPLACE 601
#define REPLICA 602
#define RESET 603
#define RESPECT_P 604
#define RESTART 605
#define RESTRICT 606
#define RETURNING 607
#define RETURNS 608
#define REVOKE 609
#define RIGHT 610
#define ROLE 611
#define ROLLBACK 612
#define ROLLUP 613
#define ROW 614
#define ROWS 615
#define RULE 616
#define SAMPLE 617
#define SAVEPOINT 618
#define SCHEMA 619
#define SCHEMAS 620
#define SCROLL 621
#define SEARCH 622
#define SECOND_P 623
#define SECONDS_P 624
#define SECURITY 625
#define SELECT 626
#define SEMI 627
#define SEQUENCE 628
#define SEQUENCES 629
#define SERIALIZABLE 630
#define SERVER 631
#define SESSION 632
#define SESSION_USER 633
#define SET 634
#define SETOF 635
#define SETS 636
#define SHARE 637
#define SHOW 638
#define SIMILAR 639
#define SIMPLE 640
#define SKIP 641
#define SMALLINT 642
#define SNAPSHOT 643
#define SOME 644
#define SQL_P 645
#define STABLE 646
#define STANDALONE_P 647
#define START 648
#define STATEMENT 649
#define STATISTICS 650
#define STDIN 651
#define STDOUT 652
#define STORAGE 653
#define STORED 654
#define STRICT_P 655
#define STRIP_P 656
#define STRUCT 657
#define SUBSCRIPTION 658
#define SUBSTRING 659
#define SUMMARIZE 660
#define SYMMETRIC 661
#define SYSID 662
#define SYSTEM_P 663
#define TABLE 664
#define TABLES 665
#define TABLESAMPLE 666
#define TABLESPACE 667
#define TEMP 668
#define TEMPLATE 669
#define TEMPORARY 670
#define TEXT_P 671
#define THEN 672
#define TIME 673
#define TIMESTAMP 674
#define TO 675
#define TRAILING 676
#define TRANSACTION 677
#define TRANSFORM 678
#define TREAT 679
#define TRIGGER 680
#define TRIM 681
#define TRUE_P 682
#define TRUNCATE 683
#define TRUSTED 684
#define TRY_CAST 685
#define TYPE_P 686
#define TYPES_P 687
#define UNBOUNDED 688
#define UNCOMMITTED 689
#define UNENCRYPTED 690
#define UNION 691
#define UNIQUE 692
#define UNKNOWN 693
#define UNLISTEN 694
#define UNLOGGED 695
#define UNTIL 696
#define UPDATE 697
#define USE_P 698
#define USER 699
#define USING 700
#define VACUUM 701
#define VALID 702
#define VALIDATE 703
#define VALIDATOR 704
#define VALUE_P 705
#define VALUES 706
#define VARCHAR 707
#define VARIADIC 708
#define VARYING 709
#define VERBOSE 710
#define VERSION_P 711
#define VIEW 712
#define VIEWS 713
#define VIRTUAL 714
#define VOLATILE 715
#define WHEN 716
#define WHERE 717
#define WHITESPACE_P 718
#define WINDOW 719
#define WITH 720
#define WITHIN 721
#define WITHOUT 722
#define WORK 723
#define WRAPPER 724
#define WRITE_P 725
#define XML_P 726
#define XMLATTRIBUTES 727
#define XMLCONCAT 728
#define XMLELEMENT 729
#define XMLEXISTS 730
#define XMLFOREST 731
#define XMLNAMESPACES 732
#define XMLPARSE 733
#define XMLPI 734
#define XMLROOT 735
#define XMLSERIALIZE 736
#define XMLTABLE 737
#define YEAR_P 738
#define YEARS_P 739
#define YES_P 740
#define ZONE 741
#define NOT_LA 742
#define NULLS_LA 743
#define WITH_LA 744
#define POSTFIXOP 745
#define UMINUS 746




#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
#line 14 "third_party/libpg_query/grammar/grammar.y"
{
	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;
	const char          *conststr;

	char				chr;
	bool				boolean;
	PGJoinType			jtype;
	PGDropBehavior		dbehavior;
	PGOnCommitAction		oncommit;
	PGOnCreateConflict		oncreateconflict;
	PGList				*list;
	PGNode				*node;
	PGValue				*value;
	PGObjectType			objtype;
	PGTypeName			*typnam;
	PGObjectWithArgs		*objwithargs;
	PGDefElem				*defelt;
	PGSortBy				*sortby;
	PGWindowDef			*windef;
	PGJoinExpr			*jexpr;
	PGIndexElem			*ielem;
	PGAlias				*alias;
	PGRangeVar			*range;
	PGIntoClause			*into;
	PGWithClause			*with;
	PGInferClause			*infer;
	PGOnConflictClause	*onconflict;
	PGOnConflictActionAlias onconflictshorthand;
	PGAIndices			*aind;
	PGResTarget			*target;
	PGInsertStmt			*istmt;
	PGVariableSetStmt		*vsetstmt;
	PGOverridingKind       override;
	PGSortByDir            sortorder;
	PGSortByNulls          nullorder;
	PGConstrType           constr;
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;
}
/* Line 1529 of yacc.c.  */
#line 1077 "third_party/libpg_query/grammar/grammar_out.hpp"
	YYSTYPE;
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
# define YYSTYPE_IS_TRIVIAL 1
#endif



#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} YYLTYPE;
# define yyltype YYLTYPE /* obsolescent; will be withdrawn */
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


