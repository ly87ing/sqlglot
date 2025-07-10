"""
达梦(DaMeng)数据库方言实现
"""
from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.oracle import Oracle
from sqlglot.tokens import TokenType


class Dameng(Oracle):
    """
    达梦数据库方言实现。
    继承自 Oracle 方言，并根据官方文档增加了达梦特有的类型、函数和关键字，
    同时为从 MySQL 等其他数据库的转换提供了兼容性映射。
    """

    # 达梦数据库的时间格式
    TIME_FORMAT = "'YYYY-MM-DD HH24:MI:SS'"
    TIME_MAPPING = {
        **Oracle.TIME_MAPPING,
        "%Y": "YYYY",
        "%y": "YY",
        "%m": "MM",
        "%d": "DD",
        "%H": "HH24",
        "%I": "HH12",
        "%M": "MI",
        "%S": "SS",
    }

    class Generator(Oracle.Generator):
        """达梦数据库的SQL生成器"""

        # 达梦数据库的数据类型映射
        TYPE_MAPPING = {
            **Oracle.Generator.TYPE_MAPPING,
            # 基础数据类型
            exp.DataType.Type.TINYINT: "TINYINT",
            exp.DataType.Type.SMALLINT: "SMALLINT",
            exp.DataType.Type.INT: "INT",
            exp.DataType.Type.BIGINT: "BIGINT",
            exp.DataType.Type.FLOAT: "FLOAT",
            exp.DataType.Type.DOUBLE: "DOUBLE",
            exp.DataType.Type.DECIMAL: "DECIMAL",
            exp.DataType.Type.NUMBER: "NUMBER",
            exp.DataType.Type.REAL: "REAL",

            # 字符串类型
            exp.DataType.Type.CHAR: "CHAR",
            exp.DataType.Type.VARCHAR: "VARCHAR",
            exp.DataType.Type.NVARCHAR: "NVARCHAR2",
            exp.DataType.Type.TEXT: "TEXT",
            exp.DataType.Type.CLOB: "CLOB",
            exp.DataType.Type.LONGTEXT: "TEXT",

            # 二进制类型
            exp.DataType.Type.BINARY: "BINARY",
            exp.DataType.Type.VARBINARY: "VARBINARY",
            exp.DataType.Type.BLOB: "BLOB",
            exp.DataType.Type.LONGBLOB: "BLOB",

            # 日期时间类型
            exp.DataType.Type.DATE: "DATE",
            exp.DataType.Type.TIME: "TIME",
            exp.DataType.Type.DATETIME: "DATETIME",
            exp.DataType.Type.TIMESTAMP: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP WITH TIME ZONE",
            exp.DataType.Type.TIMESTAMPLTZ: "TIMESTAMP WITH LOCAL TIME ZONE",

            # 其他类型
            exp.DataType.Type.BOOLEAN: "BOOLEAN",
            exp.DataType.Type.JSON: "JSON",
            exp.DataType.Type.XML: "XML",

            # MySQL兼容类型映射
            exp.DataType.Type.MEDIUMINT: "INT",
            exp.DataType.Type.MEDIUMTEXT: "TEXT",
            exp.DataType.Type.TINYTEXT: "TEXT",
            exp.DataType.Type.TINYBLOB: "BLOB",
            exp.DataType.Type.YEAR: "SMALLINT",
            exp.DataType.Type.ENUM: "VARCHAR",
            exp.DataType.Type.SET: "VARCHAR",
        }

        # 达梦数据库的基本函数转换
        TRANSFORMS = {
            **Oracle.Generator.TRANSFORMS,

            # 基本字符串函数
            exp.Concat: lambda self, e: self.func("CONCAT", *e.expressions),
            exp.Length: lambda self, e: self.func("LENGTH", e.this),
            exp.Lower: lambda self, e: self.func("LOWER", e.this),
            exp.Upper: lambda self, e: self.func("UPPER", e.this),
            exp.Trim: lambda self, e: self.func("TRIM", e.this),

            # 基本数值函数
            exp.Abs: lambda self, e: self.func("ABS", e.this),
            exp.Ceil: lambda self, e: self.func("CEIL", e.this),
            exp.Floor: lambda self, e: self.func("FLOOR", e.this),
            exp.Round: lambda self, e: self.func("ROUND", e.this, e.args.get("decimals", 0)),
            exp.Sqrt: lambda self, e: self.func("SQRT", e.this),
            exp.Pow: lambda self, e: self.func("POWER", e.this, e.expression),

            # 基本日期时间函数
            exp.CurrentDate: lambda self, e: "CURRENT_DATE",
            exp.CurrentTime: lambda self, e: "CURRENT_TIME",
            exp.CurrentTimestamp: lambda self, e: "CURRENT_TIMESTAMP",

            # 基本类型转换函数
            exp.Cast: lambda self, e: self.func("CAST", e.this, "AS", e.to),

            # 基本条件函数
            exp.Coalesce: lambda self, e: self.func("COALESCE", *e.expressions),
            exp.Nullif: lambda self, e: self.func("NULLIF", e.this, e.expression),

            # 基本聚合函数
            exp.Count: lambda self, e: self.func("COUNT", e.this or "*"),
            exp.Sum: lambda self, e: self.func("SUM", e.this),
            exp.Avg: lambda self, e: self.func("AVG", e.this),
            exp.Max: lambda self, e: self.func("MAX", e.this),
            exp.Min: lambda self, e: self.func("MIN", e.this),
        }

        def limit_sql(self, expression: exp.Limit, top: bool = False) -> str:
            """达梦数据库的LIMIT语句生成"""
            if top:
                return f"TOP {self.sql(expression, 'expression')}"
            return f"LIMIT {self.sql(expression, 'expression')}"

        def offset_sql(self, expression: exp.Offset) -> str:
            """达梦数据库的OFFSET语句生成"""
            return f"OFFSET {self.sql(expression, 'expression')}"

        def autoincrementcolumnconstraint_sql(self, expression: exp.AutoIncrementColumnConstraint) -> str:
            # 达梦使用 IDENTITY 关键字定义自增列
            # 注意：从MySQL转换时，无法直接映射种子和增量，因为MySQL的AUTO_INCREMENT没有列级参数
            return "IDENTITY"

    class Tokenizer(Oracle.Tokenizer):
        """达梦数据库的分词器"""

        KEYWORDS = {
            **Oracle.Tokenizer.KEYWORDS,
            # 达梦基本关键词
            "IDENTITY": TokenType.IDENTITY,
            "LIMIT": TokenType.LIMIT,
            "OFFSET": TokenType.OFFSET,
            "BOOLEAN": TokenType.BOOLEAN,
            "TINYINT": TokenType.TINYINT,
            "SMALLINT": TokenType.SMALLINT,
            "MEDIUMINT": TokenType.MEDIUMINT,
            "BIGINT": TokenType.BIGINT,
            "TEXT": TokenType.TEXT,
            "LONGTEXT": TokenType.LONGTEXT,
            "BLOB": TokenType.BLOB,
            "LONGBLOB": TokenType.LONGBLOB,
        }

    class Parser(Oracle.Parser):
        """达梦数据库的解析器"""

        # 达梦数据库支持的基本函数
        FUNCTIONS = {
            **Oracle.Parser.FUNCTIONS,

            # 达梦特有的自增列信息查询函数
            "IDENT_SEED": exp.Func.from_arg_list,
            "IDENT_INCR": exp.Func.from_arg_list,

            # 基本字符串函数
            "CONCAT": exp.Concat.from_arg_list,
            "LENGTH": exp.Length.from_arg_list,
            "LOWER": exp.Lower.from_arg_list,
            "UPPER": exp.Upper.from_arg_list,
            "TRIM": exp.Trim.from_arg_list,

            # 基本数值函数
            "ABS": exp.Abs.from_arg_list,
            "CEIL": exp.Ceil.from_arg_list,
            "FLOOR": exp.Floor.from_arg_list,
            "ROUND": exp.Round.from_arg_list,
            "SQRT": exp.Sqrt.from_arg_list,
            "POWER": exp.Pow.from_arg_list,

            # 基本日期时间函数
            "CURRENT_DATE": exp.CurrentDate.from_arg_list,
            "CURRENT_TIME": exp.CurrentTime.from_arg_list,
            "CURRENT_TIMESTAMP": exp.CurrentTimestamp.from_arg_list,
            "SYSDATE": exp.CurrentTimestamp.from_arg_list,

            # 基本类型转换函数
            "CAST": exp.Cast.from_arg_list,

            # 基本条件函数
            "COALESCE": exp.Coalesce.from_arg_list,
            "NULLIF": exp.Nullif.from_arg_list,

            # 基本聚合函数
            "COUNT": exp.Count.from_arg_list,
            "SUM": exp.Sum.from_arg_list,
            "AVG": exp.Avg.from_arg_list,
            "MAX": exp.Max.from_arg_list,
            "MIN": exp.Min.from_arg_list,
        } 