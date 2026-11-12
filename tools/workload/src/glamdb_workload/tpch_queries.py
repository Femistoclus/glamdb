"""Versioned conservative TPC-H source scans and bounded Q1/Q6 reductions.

Parameter source: TPC-H 3.0.1, clauses 2.4.1--2.4.22 (qualification
substitutions); https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v3.0.1.pdf
These plans describe source access patterns, not engine SQL execution.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP, localcontext
from fractions import Fraction
import sqlite3
import tempfile
from pathlib import Path

from glamdb_sdk import and_, col, or_
from glamdb_sdk.codec import decimal_parts


PROFILES = {"native", "legacy-scaled"}
REDUCTIONS = frozenset({1, 6})
DECIMAL_FIELDS = frozenset({"l_quantity", "l_extendedprice", "l_discount", "l_tax", "c_acctbal"})
RESULT_POLICY = {"scale": 2, "rounding": "ROUND_HALF_UP", "intermediate_arithmetic": "exact unbounded integer coefficients"}


def parse_queries(selection="all"):
    if selection == "all":
        return list(range(1, 23))
    if not isinstance(selection, str) or not selection:
        raise ValueError("queries must be all or distinct comma-separated numbers 1..22")
    values = selection.split(",")
    if any(not v.isascii() or not v.isdigit() for v in values):
        raise ValueError("queries must be all or distinct comma-separated numbers 1..22")
    numbers = [int(v) for v in values]
    if any(n < 1 or n > 22 for n in numbers) or len(set(numbers)) != len(numbers):
        raise ValueError("queries must be distinct numbers 1..22")
    return numbers


def query_plans(queries="all", *, profile="native", scale_factor="1", mode="scan"):
    numbers = parse_queries(queries)
    if mode not in {"scan", "reduce"}:
        raise ValueError("mode must be scan or reduce")
    unsupported = [n for n in numbers if n not in REDUCTIONS]
    if mode == "reduce" and unsupported:
        raise ValueError("unsupported reductions: " + ",".join(map(str, unsupported)) + "; supported reductions: 1,6")
    return [query_plan(n, profile=profile, scale_factor=scale_factor) for n in numbers]


def query_plan(number, *, profile="native", scale_factor="1"):
    """Compile exact SDK literal encodings; never push SQL final limits."""
    if type(number) is not int or not 1 <= number <= 22 or profile not in PROFILES:
        raise ValueError("query must be 1..22 and profile native or legacy-scaled")
    try:
        sf = Fraction(scale_factor)
    except (ValueError, TypeError, ZeroDivisionError) as error:
        raise ValueError("scale factor must be an exact positive decimal") from error
    if sf <= 0:
        raise ValueError("scale factor must be positive")

    def column(field):
        if field in DECIMAL_FIELDS:
            return col(field, "decimal", precision=15, scale=2) if profile == "native" else col(field, "int64")
        if field.endswith("date"):
            return col(field, "date" if profile == "native" else "timestamp_us_utc")
        return col(field, "int64" if field.endswith("key") or field == "p_size" else "string")

    def literal(field, value):
        if field in DECIMAL_FIELDS:
            return Decimal(str(value)) if profile == "native" else decimal_parts(str(value), 15, 2)[0]
        if field.endswith("date") and profile == "legacy-scaled":
            return value + "T00:00:00.000000Z"
        return value

    def comparison(field, op, value):
        return getattr(column(field), op)(literal(field, value))

    def eq(f, v): return comparison(f, "eq", v)
    def ne(f, v): return comparison(f, "ne", v)
    def ge(f, v): return comparison(f, "ge", v)
    def le(f, v): return comparison(f, "le", v)
    def lt(f, v): return comparison(f, "lt", v)
    def gt(f, v): return comparison(f, "gt", v)
    def inside(f, values): return column(f).in_([literal(f, v) for v in values])
    def interval(f, start, end): return and_(ge(f, start), lt(f, end))

    stages = []

    def scan(table, alias, projection, predicate=None, residual=()):
        stages.append({"stage_id": alias, "table": table, "alias": alias,
                       "projection": projection.split(),
                       "predicate": predicate.to_dict() if predicate is not None else None,
                       "residual": list(residual)})

    params, residual, final_limit = {}, [], None
    if number == 1:
        params = {"DELTA": 90, "SHIPDATE_MAX": (date(1998, 12, 1) - timedelta(days=90)).isoformat()}
        scan("lineitem", "l", "l_returnflag l_linestatus l_quantity l_extendedprice l_discount l_tax", le("l_shipdate", params["SHIPDATE_MAX"]))
        residual = ["GROUP BY l_returnflag,l_linestatus; SUM(quantity), SUM(price), SUM(price*(1-discount)), SUM(price*(1-discount)*(1+tax)); AVG(quantity), AVG(price), AVG(discount), COUNT(*)", "ORDER BY l_returnflag,l_linestatus"]
    elif number == 2:
        params = {"SIZE": 15, "TYPE": "BRASS", "REGION": "EUROPE"}
        scan("part", "p", "p_partkey p_mfgr p_type", eq("p_size", 15), ["p_type LIKE '%BRASS'"])
        scan("partsupp", "ps", "ps_partkey ps_suppkey ps_supplycost")
        scan("supplier", "s", "s_suppkey s_nationkey s_acctbal s_name s_address s_phone s_comment")
        scan("nation", "n", "n_nationkey n_regionkey n_name")
        scan("region", "r", "r_regionkey", eq("r_name", "EUROPE"))
        scan("partsupp", "ps_min", "ps_partkey ps_suppkey ps_supplycost")
        scan("supplier", "s_min", "s_suppkey s_nationkey")
        scan("nation", "n_min", "n_nationkey n_regionkey")
        scan("region", "r_min", "r_regionkey", eq("r_name", "EUROPE"))
        residual = ["p_partkey=ps_partkey AND ps_suppkey=s_suppkey AND s_nationkey=n_nationkey AND n_regionkey=r_regionkey", "ps_supplycost = correlated MIN(ps_min.ps_supplycost) joined through s_min,n_min,r_min for p_partkey", "ORDER BY s_acctbal DESC,n_name,s_name,p_partkey; final first 100 rows"]
        final_limit = 100
    elif number == 3:
        params = {"SEGMENT": "BUILDING", "DATE": "1995-03-15"}
        scan("customer", "c", "c_custkey", eq("c_mktsegment", "BUILDING"))
        scan("orders", "o", "o_orderkey o_custkey o_orderdate o_shippriority", lt("o_orderdate", params["DATE"]))
        scan("lineitem", "l", "l_orderkey l_extendedprice l_discount", gt("l_shipdate", params["DATE"]))
        residual = ["c_custkey=o_custkey AND l_orderkey=o_orderkey", "GROUP BY l_orderkey,o_orderdate,o_shippriority; SUM(l_extendedprice*(1-l_discount)) AS revenue", "ORDER BY revenue DESC,o_orderdate; final first 10 rows"]
        final_limit = 10
    elif number == 4:
        params = {"DATE": "1993-07-01"}
        scan("orders", "o", "o_orderkey o_orderpriority", interval("o_orderdate", "1993-07-01", "1993-10-01"))
        scan("lineitem", "l", "l_orderkey l_commitdate l_receiptdate", residual=["l_commitdate < l_receiptdate"])
        residual = ["EXISTS lineitem WHERE l_orderkey=o_orderkey AND l_commitdate<l_receiptdate", "GROUP BY o_orderpriority; COUNT(*); ORDER BY o_orderpriority"]
    elif number == 5:
        params = {"REGION": "ASIA", "DATE": "1994-01-01"}
        scan("customer", "c", "c_custkey c_nationkey")
        scan("orders", "o", "o_orderkey o_custkey", interval("o_orderdate", "1994-01-01", "1995-01-01"))
        scan("lineitem", "l", "l_orderkey l_suppkey l_extendedprice l_discount")
        scan("supplier", "s", "s_suppkey s_nationkey")
        scan("nation", "n", "n_nationkey n_regionkey n_name")
        scan("region", "r", "r_regionkey", eq("r_name", "ASIA"))
        residual = ["c_custkey=o_custkey AND l_orderkey=o_orderkey AND l_suppkey=s_suppkey AND c_nationkey=s_nationkey AND s_nationkey=n_nationkey AND n_regionkey=r_regionkey", "GROUP BY n_name; SUM(l_extendedprice*(1-l_discount)) AS revenue; ORDER BY revenue DESC"]
    elif number == 6:
        params = {"DATE": "1994-01-01", "DISCOUNT": "0.06", "QUANTITY": "24.00"}
        scan("lineitem", "l", "l_extendedprice l_discount", and_(ge("l_shipdate", "1994-01-01"), lt("l_shipdate", "1995-01-01"), ge("l_discount", "0.05"), le("l_discount", "0.07"), lt("l_quantity", "24.00")))
        residual = ["SUM(l_extendedprice*l_discount) AS revenue; SQL empty/all-NULL input returns NULL"]
    elif number == 7:
        params = {"NATION1": "FRANCE", "NATION2": "GERMANY"}
        scan("supplier", "s", "s_suppkey s_nationkey")
        scan("lineitem", "l", "l_suppkey l_orderkey l_shipdate l_extendedprice l_discount", and_(ge("l_shipdate", "1995-01-01"), le("l_shipdate", "1996-12-31")))
        scan("orders", "o", "o_orderkey o_custkey")
        scan("customer", "c", "c_custkey c_nationkey")
        for alias in ("n1", "n2"):
            scan("nation", alias, "n_nationkey n_name", inside("n_name", ["FRANCE", "GERMANY"]))
        residual = ["s_suppkey=l_suppkey AND o_orderkey=l_orderkey AND c_custkey=o_custkey AND s_nationkey=n1.n_nationkey AND c_nationkey=n2.n_nationkey", "(n1.name=FRANCE AND n2.name=GERMANY) OR (n1.name=GERMANY AND n2.name=FRANCE)", "EXTRACT YEAR(l_shipdate); GROUP BY supplier nation,customer nation,year; SUM(l_extendedprice*(1-l_discount)); ORDER BY supplier nation,customer nation,year"]
    elif number == 8:
        params = {"NATION": "BRAZIL", "REGION": "AMERICA", "TYPE": "ECONOMY ANODIZED STEEL"}
        scan("part", "p", "p_partkey", eq("p_type", params["TYPE"]))
        scan("supplier", "s", "s_suppkey s_nationkey")
        scan("lineitem", "l", "l_partkey l_suppkey l_orderkey l_extendedprice l_discount")
        scan("orders", "o", "o_orderkey o_custkey o_orderdate", and_(ge("o_orderdate", "1995-01-01"), le("o_orderdate", "1996-12-31")))
        scan("customer", "c", "c_custkey c_nationkey")
        scan("nation", "n1", "n_nationkey n_regionkey")
        scan("nation", "n2", "n_nationkey n_name")
        scan("region", "r", "r_regionkey", eq("r_name", "AMERICA"))
        residual = ["p_partkey=l_partkey AND s_suppkey=l_suppkey AND l_orderkey=o_orderkey AND o_custkey=c_custkey AND c_nationkey=n1.n_nationkey AND n1.n_regionkey=r_regionkey AND s_nationkey=n2.n_nationkey", "GROUP BY EXTRACT YEAR(o_orderdate); SUM(CASE n2.name=BRAZIL THEN volume ELSE 0 END)/SUM(volume), volume=l_extendedprice*(1-l_discount); ORDER BY year; denominator retains other supplier nations"]
    elif number == 9:
        params = {"COLOR": "green"}
        scan("part", "p", "p_partkey p_name", residual=["p_name LIKE '%green%'"])
        scan("supplier", "s", "s_suppkey s_nationkey")
        scan("lineitem", "l", "l_partkey l_suppkey l_orderkey l_extendedprice l_discount l_quantity")
        scan("partsupp", "ps", "ps_partkey ps_suppkey ps_supplycost")
        scan("orders", "o", "o_orderkey o_orderdate")
        scan("nation", "n", "n_nationkey n_name")
        residual = ["s_suppkey=l_suppkey AND ps_suppkey=l_suppkey AND ps_partkey=l_partkey AND p_partkey=l_partkey AND o_orderkey=l_orderkey AND s_nationkey=n_nationkey", "GROUP BY nation,EXTRACT YEAR(o_orderdate); SUM(l_extendedprice*(1-l_discount)-ps_supplycost*l_quantity); ORDER BY nation,year DESC"]
    elif number == 10:
        params = {"DATE": "1993-10-01"}
        scan("customer", "c", "c_custkey c_name c_acctbal c_phone c_address c_comment c_nationkey")
        scan("orders", "o", "o_orderkey o_custkey", interval("o_orderdate", "1993-10-01", "1994-01-01"))
        scan("lineitem", "l", "l_orderkey l_extendedprice l_discount", eq("l_returnflag", "R"))
        scan("nation", "n", "n_nationkey n_name")
        residual = ["c_custkey=o_custkey AND l_orderkey=o_orderkey AND c_nationkey=n_nationkey", "GROUP BY c_custkey,c_name,c_acctbal,c_phone,n_name,c_address,c_comment; SUM(l_extendedprice*(1-l_discount)) AS revenue; ORDER BY revenue DESC; final first 20 rows"]
        final_limit = 20
    elif number == 11:
        fraction = Fraction(1, 10000) / sf
        params = {"NATION": "GERMANY", "FRACTION": {"numerator": str(fraction.numerator), "denominator": str(fraction.denominator), "formula": "0.0001 / SF"}}
        for suffix in ("", "_total"):
            scan("partsupp", "ps" + suffix, ("ps_partkey " if not suffix else "") + "ps_suppkey ps_supplycost ps_availqty")
            scan("supplier", "s" + suffix, "s_suppkey s_nationkey")
            scan("nation", "n" + suffix, "n_nationkey", eq("n_name", "GERMANY"))
        residual = ["ps_suppkey=s_suppkey AND s_nationkey=n_nationkey in both grouped and total subqueries", "GROUP BY ps_partkey; SUM(ps_supplycost*ps_availqty) AS value; HAVING value > total inventory value * exact FRACTION; ORDER BY value DESC"]
    elif number == 12:
        params = {"SHIPMODE1": "MAIL", "SHIPMODE2": "SHIP", "DATE": "1994-01-01"}
        scan("orders", "o", "o_orderkey o_orderpriority")
        scan("lineitem", "l", "l_orderkey l_shipmode l_commitdate l_receiptdate l_shipdate", and_(inside("l_shipmode", ["MAIL", "SHIP"]), ge("l_receiptdate", "1994-01-01"), lt("l_receiptdate", "1995-01-01")), ["l_commitdate<l_receiptdate AND l_shipdate<l_commitdate"])
        residual = ["o_orderkey=l_orderkey; l_commitdate<l_receiptdate AND l_shipdate<l_commitdate", "GROUP BY l_shipmode; SUM(CASE priority IN (1-URGENT,2-HIGH) THEN 1 ELSE 0 END), SUM(CASE priority<>1-URGENT AND priority<>2-HIGH THEN 1 ELSE 0 END); ORDER BY l_shipmode"]
    elif number == 13:
        params = {"WORD1": "special", "WORD2": "requests"}
        scan("customer", "c", "c_custkey")
        scan("orders", "o", "o_orderkey o_custkey o_comment")
        residual = ["customer LEFT OUTER JOIN orders ON c_custkey=o_custkey AND o_comment NOT LIKE '%special%requests%'; retain customers with zero matching orders", "GROUP BY c_custkey; COUNT(o_orderkey) AS c_count; GROUP BY c_count; COUNT(*) AS custdist; ORDER BY custdist DESC,c_count DESC"]
    elif number == 14:
        params = {"DATE": "1995-09-01"}
        scan("lineitem", "l", "l_partkey l_extendedprice l_discount", interval("l_shipdate", "1995-09-01", "1995-10-01"))
        scan("part", "p", "p_partkey p_type")
        residual = ["l_partkey=p_partkey", "100*SUM(CASE p_type LIKE 'PROMO%' THEN l_extendedprice*(1-l_discount) ELSE 0 END)/SUM(l_extendedprice*(1-l_discount)); denominator includes non-PROMO parts"]
    elif number == 15:
        params = {"DATE": "1996-01-01"}
        scan("lineitem", "l", "l_suppkey l_extendedprice l_discount", interval("l_shipdate", "1996-01-01", "1996-04-01"))
        scan("supplier", "s", "s_suppkey s_name s_address s_phone")
        residual = ["revenue view GROUP BY l_suppkey; SUM(l_extendedprice*(1-l_discount)) AS total_revenue", "s_suppkey=supplier_no AND total_revenue=MAX(total_revenue), preserving ties; ORDER BY s_suppkey"]
    elif number == 16:
        sizes = [49, 14, 23, 45, 19, 3, 36, 9]
        params = {"BRAND": "Brand#45", "TYPE": "MEDIUM POLISHED", "SIZES": sizes}
        scan("part", "p", "p_partkey p_brand p_type p_size", and_(ne("p_brand", "Brand#45"), inside("p_size", sizes)), ["p_type NOT LIKE 'MEDIUM POLISHED%'"])
        scan("partsupp", "ps", "ps_partkey ps_suppkey")
        scan("supplier", "s_bad", "s_suppkey s_comment", residual=["s_comment LIKE '%Customer%Complaints%'"])
        residual = ["p_partkey=ps_partkey; ps_suppkey NOT IN suppliers matching s_comment LIKE '%Customer%Complaints%' (SQL NULL semantics)", "GROUP BY p_brand,p_type,p_size; COUNT(DISTINCT ps_suppkey) AS supplier_cnt; ORDER BY supplier_cnt DESC,p_brand,p_type,p_size"]
    elif number == 17:
        params = {"BRAND": "Brand#23", "CONTAINER": "MED BOX"}
        scan("lineitem", "l", "l_partkey l_quantity l_extendedprice")
        scan("part", "p", "p_partkey", and_(eq("p_brand", "Brand#23"), eq("p_container", "MED BOX")))
        scan("lineitem", "l_avg", "l_partkey l_quantity")
        residual = ["p_partkey=l_partkey; l_quantity<0.2*correlated AVG(l_avg.l_quantity) WHERE l_avg.l_partkey=p_partkey", "SUM(l_extendedprice)/7.0 AS avg_yearly"]
    elif number == 18:
        params = {"QUANTITY": "300.00"}
        scan("customer", "c", "c_custkey c_name")
        scan("orders", "o", "o_orderkey o_custkey o_orderdate o_totalprice")
        scan("lineitem", "l", "l_orderkey l_quantity")
        scan("lineitem", "l_large", "l_orderkey l_quantity")
        residual = ["o_orderkey IN (l_large GROUP BY l_orderkey HAVING SUM(l_quantity)>300); c_custkey=o_custkey AND o_orderkey=l_orderkey", "GROUP BY c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice; SUM(l_quantity); ORDER BY o_totalprice DESC,o_orderdate; final first 100 rows"]
        final_limit = 100
    elif number == 19:
        brands, quantities = ["Brand#12", "Brand#23", "Brand#34"], [1, 10, 20]
        params = {"BRANDS": brands, "QUANTITIES": [str(q) + ".00" for q in quantities]}
        containers = [["SM CASE", "SM BOX", "SM PACK", "SM PKG"], ["MED BAG", "MED BOX", "MED PKG", "MED PACK"], ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]]
        branches = [and_(eq("p_brand", b), inside("p_container", values), ge("p_size", 1), le("p_size", limit)) for b, values, limit in zip(brands, containers, [5, 10, 15])]
        scan("part", "p", "p_partkey p_brand p_container p_size", or_(*branches))
        qbranches = [and_(ge("l_quantity", q), le("l_quantity", q + 10)) for q in quantities]
        scan("lineitem", "l", "l_partkey l_extendedprice l_discount l_quantity", and_(or_(*qbranches), inside("l_shipmode", ["AIR", "AIR REG"]), eq("l_shipinstruct", "DELIVER IN PERSON")))
        residual = ["p_partkey=l_partkey AND coupled OR branches: (Brand#12,SM containers,p_size 1..5,l_quantity 1..11) OR (Brand#23,MED containers,p_size 1..10,l_quantity 10..20) OR (Brand#34,LG containers,p_size 1..15,l_quantity 20..30); independent candidates do not establish a branch match", "SUM(l_extendedprice*(1-l_discount)) AS revenue"]
    elif number == 20:
        params = {"COLOR": "forest", "DATE": "1994-01-01", "NATION": "CANADA"}
        scan("supplier", "s", "s_suppkey s_nationkey s_name s_address")
        scan("nation", "n", "n_nationkey", eq("n_name", "CANADA"))
        scan("partsupp", "ps", "ps_partkey ps_suppkey ps_availqty")
        scan("part", "p", "p_partkey p_name", residual=["p_name LIKE 'forest%'"])
        scan("lineitem", "l_sum", "l_partkey l_suppkey l_quantity", interval("l_shipdate", "1994-01-01", "1995-01-01"))
        residual = ["s_nationkey=n_nationkey; s_suppkey IN ps_suppkey WHERE ps_partkey IN parts matching forest prefix", "ps_availqty>0.5*correlated SUM(l_sum.l_quantity) WHERE l_partkey=ps_partkey AND l_suppkey=ps_suppkey; ORDER BY s_name"]
    elif number == 21:
        params = {"NATION": "SAUDI ARABIA"}
        scan("supplier", "s", "s_suppkey s_nationkey s_name")
        scan("lineitem", "l1", "l_orderkey l_suppkey l_receiptdate l_commitdate", residual=["l1.l_receiptdate>l1.l_commitdate"])
        scan("orders", "o", "o_orderkey", eq("o_orderstatus", "F"))
        scan("nation", "n", "n_nationkey", eq("n_name", "SAUDI ARABIA"))
        scan("lineitem", "l2", "l_orderkey l_suppkey")
        scan("lineitem", "l3", "l_orderkey l_suppkey l_receiptdate l_commitdate", residual=["l3.l_receiptdate>l3.l_commitdate"])
        residual = ["s_suppkey=l1.l_suppkey AND o_orderkey=l1.l_orderkey AND s_nationkey=n_nationkey AND l1.l_receiptdate>l1.l_commitdate", "EXISTS l2 WHERE l2.l_orderkey=l1.l_orderkey AND l2.l_suppkey<>l1.l_suppkey; NOT EXISTS l3 WHERE same order,different supplier AND l3.l_receiptdate>l3.l_commitdate", "GROUP BY s_name; COUNT(*) AS numwait; ORDER BY numwait DESC,s_name; final first 100 rows"]
        final_limit = 100
    elif number == 22:
        params = {"COUNTRY_CODES": ["13", "31", "23", "29", "30", "18", "17"]}
        scan("customer", "c", "c_custkey c_phone c_acctbal", residual=["SUBSTRING(c_phone,1,2) IN country codes"])
        scan("customer", "c_avg", "c_phone c_acctbal", gt("c_acctbal", "0.00"), ["SUBSTRING(c_phone,1,2) IN country codes"])
        scan("orders", "o", "o_custkey")
        residual = ["c_acctbal>AVG(c_avg.c_acctbal) for positive balances and matching phone country codes; NOT EXISTS orders WHERE o_custkey=c_custkey", "GROUP BY SUBSTRING(c_phone,1,2); COUNT(*),SUM(c_acctbal); ORDER BY country code"]
    return {"version": 1, "spec_version": "3.0.1", "query_id": number,
            "parameter_policy": "fixed-qualification", "parameters": params,
            "scale_factor": str(scale_factor), "type_profile": profile,
            "stages": stages, "residual": residual, "final_limit": final_limit,
            "reduction_supported": number in REDUCTIONS,
            "result_policy": RESULT_POLICY if number in REDUCTIONS else None,
            "engine_work": "fixed-snapshot source filtering and projection",
            "scan_result": "candidate rows; SQL answer is not verified"}


def _coefficient(value, profile):
    if value is None:
        return None
    if profile == "legacy-scaled":
        if type(value) is not int:
            raise ValueError("legacy-scaled reducer requires integer coefficients")
        return value
    return decimal_parts(value, 15, 2)[0]


def _rounded(numerator, denominator=100):
    """Round an exact rational to two decimal places, ties away from zero."""
    if numerator is None:
        return None
    sign = "-" if numerator < 0 else ""
    quotient, remainder = divmod(abs(numerator) * 100, denominator)
    quotient += remainder * 2 >= denominator
    # SQL decimal has no negative-zero representation.
    return (sign if quotient else "") + str(quotient // 100) + "." + str(quotient % 100).zfill(2)


class StreamingReducer:
    """Only one batch plus at most sixteen Q1 groups is retained."""

    def __init__(self, query, profile):
        self.query, self.profile = query, profile
        self.groups = {}
        self.revenue = None

    def add_batch(self, batch):
        # Column scalars avoid materializing a Python list for the whole batch.
        names, columns = batch.schema.names, batch.columns
        self.add_rows({name: column[index].as_py() for name, column in zip(names, columns)} for index in range(batch.num_rows))

    def add_rows(self, rows):
        for row in rows:
            if self.query == 6:
                price, discount = (_coefficient(row.get(field), self.profile) for field in ("l_extendedprice", "l_discount"))
                if price is not None and discount is not None:
                    self.revenue = (self.revenue or 0) + price * discount
                continue
            key = (row.get("l_returnflag"), row.get("l_linestatus"))
            if key not in self.groups:
                if len(self.groups) >= 16:
                    raise ValueError("Q1 group bound exceeded (TPC-H domains require at most 6 groups)")
                self.groups[key] = {"count": 0, "sums": [None] * 4, "avg_sums": [0] * 3, "avg_counts": [0] * 3}
            group = self.groups[key]
            group["count"] += 1
            qty, price, discount, tax = (_coefficient(row.get(field), self.profile) for field in ("l_quantity", "l_extendedprice", "l_discount", "l_tax"))
            disc_price = price * (100 - discount) if price is not None and discount is not None else None
            charge = disc_price * (100 + tax) if disc_price is not None and tax is not None else None
            for i, value in enumerate((qty, price, disc_price, charge)):
                if value is not None:
                    group["sums"][i] = (group["sums"][i] or 0) + value
            for i, value in enumerate((qty, price, discount)):
                if value is not None:
                    group["avg_sums"][i] += value
                    group["avg_counts"][i] += 1

    def finish(self):
        if self.query == 6:
            return {"revenue": _rounded(self.revenue, 10000)}
        result = []
        for key in sorted(self.groups, key=lambda k: tuple((v is not None, v or "") for v in k)):
            group = self.groups[key]
            row = dict(zip(("l_returnflag", "l_linestatus"), key))
            row.update((name, _rounded(value, denominator)) for name, value, denominator in zip(("sum_qty", "sum_base_price", "sum_disc_price", "sum_charge"), group["sums"], (100, 100, 10000, 1000000)))
            row.update((name, _rounded(total, count * 100) if count else None) for name, total, count in zip(("avg_qty", "avg_price", "avg_disc"), group["avg_sums"], group["avg_counts"]))
            row["count_order"] = group["count"]
            result.append(row)
        return result


def make_reducer(query, profile="native"):
    if query not in REDUCTIONS or profile not in PROFILES:
        raise ValueError("supported reductions: 1,6; profile must be native or legacy-scaled")
    return StreamingReducer(query, profile)


def reference_results(dataset, plans):
    """Evaluate source data independently using disk-backed SQLite and Decimal.

    SQL supplies the WHERE, GROUP BY, COUNT, SUM/AVG NULL semantics and ordering.
    Decimal aggregates avoid SQLite binary floating point and int64 SUM overflow.
    Neither the scan predicates nor the production reducer are reused.
    """
    selected = {plan["query_id"]: plan for plan in plans}
    if set(selected) - REDUCTIONS:
        raise ValueError("independent reference supports only Q1 and Q6")

    class Sum:
        def __init__(self): self.value = None
        def step(self, value):
            if value is not None:
                self.value = (self.value or Decimal(0)) + Decimal(str(value))
        def finalize(self): return None if self.value is None else str(self.value)

    class Average:
        def __init__(self): self.value, self.count = Decimal(0), 0
        def step(self, value):
            if value is not None:
                self.value += Decimal(str(value))
                self.count += 1
        def finalize(self): return str(self.value / self.count) if self.count else None

    def discounted(price, discount):
        return None if price is None or discount is None else str(Decimal(price) * (Decimal(1) - Decimal(discount)))

    def charge(price, discount, tax):
        value = discounted(price, discount)
        return None if value is None or tax is None else str(Decimal(value) * (Decimal(1) + Decimal(tax)))

    def product(a, b): return None if a is None or b is None else str(Decimal(a) * Decimal(b))
    def money(value): return None if value is None else format(Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), "f")

    def source_decimal(value):
        if value is None:
            return None
        value = Decimal(str(value))
        return str(value / 100 if dataset.profile == "legacy-scaled" else value)

    def source_date(value):
        if value is None:
            return None
        if isinstance(value, datetime): return value.date().isoformat()
        if isinstance(value, date): return value.isoformat()
        if isinstance(value, int): return (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=value)).date().isoformat()
        return str(value)[:10]

    result = {}
    with localcontext() as context, tempfile.TemporaryDirectory(prefix="tpch-reference-") as temporary:
        context.prec = 100
        with sqlite3.connect(Path(temporary) / "reference.sqlite") as db:
            db.execute("PRAGMA cache_size=-2048")
            db.execute("PRAGMA temp_store=FILE")
            db.execute("CREATE TABLE lineitem(flag TEXT,status TEXT,qty TEXT,price TEXT,discount TEXT,tax TEXT,shipdate TEXT)")
            db.create_aggregate("decimal_sum", 1, Sum)
            db.create_aggregate("decimal_avg", 1, Average)
            db.create_function("discounted", 2, discounted)
            db.create_function("charge", 3, charge)
            db.create_function("product", 2, product)
            db.create_function("q6_match", 2, lambda q, d: int(q is not None and d is not None and Decimal(q) < 24 and Decimal("0.05") <= Decimal(d) <= Decimal("0.07")))
            for rows in dataset.tables["lineitem"].iter_parts():
                db.executemany("INSERT INTO lineitem VALUES(?,?,?,?,?,?,?)", ((r.get("l_returnflag"), r.get("l_linestatus"), *(source_decimal(r.get(f)) for f in ("l_quantity", "l_extendedprice", "l_discount", "l_tax")), source_date(r.get("l_shipdate"))) for r in rows))
            if 1 in selected:
                columns = ("l_returnflag", "l_linestatus", "sum_qty", "sum_base_price", "sum_disc_price", "sum_charge", "avg_qty", "avg_price", "avg_disc", "count_order")
                cursor = db.execute("SELECT flag,status,decimal_sum(qty),decimal_sum(price),decimal_sum(discounted(price,discount)),decimal_sum(charge(price,discount,tax)),decimal_avg(qty),decimal_avg(price),decimal_avg(discount),COUNT(*) FROM lineitem WHERE shipdate<=? GROUP BY flag,status ORDER BY flag,status", (selected[1]["parameters"]["SHIPDATE_MAX"],))
                result["1"] = [dict(zip(columns, [*row[:2], *(money(v) for v in row[2:9]), row[9]])) for row in cursor]
            if 6 in selected:
                revenue = db.execute("SELECT decimal_sum(product(price,discount)) FROM lineitem WHERE shipdate>=? AND shipdate<? AND q6_match(qty,discount)", ("1994-01-01", "1995-01-01")).fetchone()[0]
                result["6"] = {"revenue": money(revenue)}
    return result


def reference_candidates(dataset, plans):
    """Independently count each source alias outside the timed workload.

    The descriptor's native predicate is translated to parameterized SQLite
    WHERE expressions. SQLite supplies three-valued boolean logic; native
    decimals become exact integer coefficients, dates become calendar ordinals,
    and legacy timestamps remain exact UTC microseconds. Only predicate columns
    and one bounded input part are retained, with a 2 MiB SQLite page cache.
    Residual expressions and relational operators deliberately do not filter
    candidates. This validates source access, not a SQL query answer.
    """
    by_table, results = {}, {}
    for plan in plans:
        number = str(plan["query_id"])
        if number in results:
            raise ValueError("candidate reference requires distinct query IDs")
        results[number] = {}
        for stage in plan["stages"]:
            alias = stage["stage_id"]
            if alias in results[number]:
                raise ValueError("candidate reference requires distinct source aliases")
            results[number][alias] = None
            by_table.setdefault(stage["table"], []).append((number, stage))

    def identifier(name):
        return '"' + name.replace('"', '""') + '"'

    def source_value(field, value):
        if isinstance(value, dict):
            if value.get("type") != field.type:
                raise ValueError("candidate literal type differs from source field")
            if field.type == "decimal" and (value.get("precision") != field.precision or value.get("scale") != field.scale):
                raise ValueError("candidate decimal literal parameters differ from source field")
            value = value["value"]
        if value is None:
            return None
        if field.type == "decimal":
            numerator, denominator = Decimal(str(value)).as_integer_ratio()
            coefficient, remainder = divmod(numerator * 10 ** field.scale, denominator)
            if remainder:
                raise ValueError("candidate decimal value exceeds source scale")
            return coefficient
        if field.type == "date":
            return (value if isinstance(value, date) else date.fromisoformat(value)).toordinal()
        if field.type == "timestamp_us_utc":
            if type(value) is int:
                return value
            value = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
            if value.tzinfo is None:
                raise ValueError("candidate timestamp requires an explicit UTC offset")
            delta = value.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
            return delta.days * 86400000000 + delta.seconds * 1000000 + delta.microseconds
        if field.type == "int64":
            return int(value)
        if field.type == "bool":
            return int(value)
        if field.type == "string":
            return value
        raise ValueError("candidate reference encountered an unsupported source type")

    def compile_where(predicate, fields):
        if predicate is None or predicate["op"] == "true":
            return "1", [], set()
        operation = predicate["op"]
        if operation in {"and", "or"}:
            children = [compile_where(p, fields) for p in predicate["args"]]
            if not children:
                raise ValueError("candidate compound predicates require operands")
            return ("(" + (" AND " if operation == "and" else " OR ").join(c[0] for c in children) + ")", [v for c in children for v in c[1]], set().union(*(c[2] for c in children)))
        if operation == "not":
            sql, values, names = compile_where(predicate["arg"], fields)
            return "(NOT " + sql + ")", values, names
        name = predicate["field"]
        field, sql_name = fields[name], identifier(name)
        if operation in {"is_null", "is_not_null"}:
            return sql_name + (" IS NULL" if operation == "is_null" else " IS NOT NULL"), [], {name}
        if operation == "in":
            values = [source_value(field, v) for v in predicate["values"]]
            if not values:
                # Empty IN still yields UNKNOWN for a NULL source value.
                return "(CASE WHEN " + sql_name + " IS NULL THEN NULL ELSE 0 END)", [], {name}
            return sql_name + " IN (" + ",".join("?" for _ in values) + ")", values, {name}
        operators = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
        if operation not in operators:
            raise ValueError("unsupported candidate predicate operation: " + operation)
        return sql_name + " " + operators[operation] + " ?", [source_value(field, predicate["value"])], {name}

    with tempfile.TemporaryDirectory(prefix="tpch-candidates-") as temporary:
        with sqlite3.connect(Path(temporary) / "candidates.sqlite") as db:
            db.execute("PRAGMA cache_size=-2048")
            db.execute("PRAGMA temp_store=FILE")
            for table, stages in by_table.items():
                source = dataset.tables[table]
                fields = {field.name: field for field in source.schema.fields}
                predicates = [(number, stage["stage_id"], *compile_where(stage["predicate"], fields)) for number, stage in stages]
                selected = sorted(set().union(*(item[4] for item in predicates)))
                declarations = [identifier(name) + (" TEXT" if fields[name].type == "string" else " INTEGER") for name in selected]
                db.execute("CREATE TABLE source(" + ",".join(declarations or ["unused INTEGER"]) + ")")
                insert = "INSERT INTO source VALUES(" + ",".join("?" for _ in (selected or [None])) + ")"
                for rows in source.iter_parts():
                    db.executemany(insert, (tuple(source_value(fields[name], row[name]) for name in selected) if selected else (0,) for row in rows))
                for number, alias, sql, values, _ in predicates:
                    results[number][alias] = db.execute("SELECT COUNT(*) FROM source WHERE " + sql, values).fetchone()[0]
                db.execute("DROP TABLE source")
    return results
