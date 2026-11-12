"""Logical column facts from pinned ClickBench 7dd9941f45f5f0af088cdaf8b9da3a6156a18bdc.

Reference: ClickHouse/ClickBench, clickhouse/create.sql. Preserve upstream
attribution and its CC BY-NC-SA 4.0 license when fetching benchmark material.
"""

from datetime import date, datetime, timedelta, timezone

from glamdb_sdk import Field, Schema

UPSTREAM = "7dd9941f45f5f0af088cdaf8b9da3a6156a18bdc"
REFERENCE_ROWS = 99997497
COLUMNS = tuple("""
WatchID JavaEnable Title GoodEvent EventTime EventDate CounterID ClientIP RegionID UserID
CounterClass OS UserAgent URL Referer IsRefresh RefererCategoryID RefererRegionID
URLCategoryID URLRegionID ResolutionWidth ResolutionHeight ResolutionDepth FlashMajor
FlashMinor FlashMinor2 NetMajor NetMinor UserAgentMajor UserAgentMinor CookieEnable
JavascriptEnable IsMobile MobilePhone MobilePhoneModel Params IPNetworkID TraficSourceID
SearchEngineID SearchPhrase AdvEngineID IsArtifical WindowClientWidth WindowClientHeight
ClientTimeZone ClientEventTime SilverlightVersion1 SilverlightVersion2 SilverlightVersion3
SilverlightVersion4 PageCharset CodeVersion IsLink IsDownload IsNotBounce FUniqID OriginalURL
HID IsOldCounter IsEvent IsParameter DontCountHits WithHash HitColor LocalEventTime Age Sex
Income Interests Robotness RemoteIP WindowName OpenerName HistoryLength BrowserLanguage
BrowserCountry SocialNetwork SocialAction HTTPError SendTiming DNSTiming ConnectTiming
ResponseStartTiming ResponseEndTiming FetchTiming SocialSourceNetworkID SocialSourcePage
ParamPrice ParamOrderID ParamCurrency ParamCurrencyID OpenstatServiceName OpenstatCampaignID
OpenstatAdID OpenstatSourceID UTMSource UTMMedium UTMCampaign UTMContent UTMTerm FromTag
HasGCLID RefererHash URLHash CLID
""".split())
STRING_COLUMNS = frozenset("""
Title URL Referer FlashMinor2 UserAgentMinor MobilePhoneModel Params SearchPhrase PageCharset
OriginalURL HitColor BrowserLanguage BrowserCountry SocialNetwork SocialAction SocialSourcePage
ParamOrderID ParamCurrency OpenstatServiceName OpenstatCampaignID OpenstatAdID OpenstatSourceID
UTMSource UTMMedium UTMCampaign UTMContent UTMTerm FromTag
""".split())
TIME_COLUMNS = frozenset(("EventTime", "ClientEventTime", "LocalEventTime"))
assert len(COLUMNS) == len(set(COLUMNS)) == 105
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def schema_for(profile):
    if profile not in ("legacy-date", "native"):
        raise ValueError("unknown ClickBench type profile")
    return Schema([Field(name, "string" if name in STRING_COLUMNS else
                         "date" if name == "EventDate" and profile == "native" else
                         "timestamp_us_utc" if name in TIME_COLUMNS or name == "EventDate" else "int64")
                   for name in COLUMNS])


def convert_row(row, *, type_profile="legacy-date"):
    result = {}
    for name in COLUMNS:
        value = row[name]
        if value is None:
            raise ValueError("ClickBench required column contains NULL")
        if name in STRING_COLUMNS:
            if isinstance(value, bytes):
                value = value.decode("utf-8", errors="strict")
            if not isinstance(value, str):
                raise ValueError("ClickBench text column has an incompatible physical type")
        elif name == "EventDate":
            if type(value) is int:
                text = str(value)
                if len(text) != 8:
                    raise ValueError("EventDate integer must be YYYYMMDD as in the upstream loader")
                value = date(int(text[:4]), int(text[4:6]), int(text[6:]))
            elif isinstance(value, str):
                value = date.fromisoformat(value)
            if type(value) is not date:
                raise ValueError("EventDate requires a calendar date")
            if type_profile == "legacy-date":
                value = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
        elif name in TIME_COLUMNS:
            if type(value) is int:
                value = EPOCH + timedelta(seconds=value)
            if not isinstance(value, datetime):
                raise ValueError("ClickBench timestamp requires source epoch seconds or a logical timestamp")
            value = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
        elif type(value) is not int or not -(2**63) <= value < 2**63:
            raise ValueError("ClickBench integer is outside the exact signed int64 domain")
        result[name] = value
    return result
