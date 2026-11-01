"""Validation of the serde wire types and tasks-proto::MailTask::validate."""
import copy
import time
from typing import Any, Dict, Mapping
from .errors import InvalidRequestError


def invalid(message: str) -> None:
    raise InvalidRequestError(message, status=400)


def string(obj: Mapping[str, Any], key: str, prefix: str = "") -> str:
    value = obj.get(key, "")
    if not isinstance(value, str):
        invalid(prefix + key + " must be a string")
    if len(value) > 1024:
        invalid(prefix + key + " exceeds 1024 characters")
    return value


def enum(value: str, choices: tuple, field: str) -> None:
    if value not in choices:
        invalid(field + " has invalid value " + repr(value))


def prepare(task: Mapping[str, Any], group: str) -> Dict[str, Any]:
    if not isinstance(task, Mapping):
        invalid("task must be an object")
    body = copy.deepcopy(dict(task))
    body.setdefault("group", group)
    body.setdefault("receivedAt", int(time.time()))
    body.setdefault("timeout", 300)
    for key in ("email", "group", "templateId"):
        string(body, key)
    if not body.get("email", "").strip():
        invalid("email is required")
    for key, maximum in (("receivedAt", 2**63 - 1), ("timeout", 2**32 - 1)):
        value = body[key]
        if type(value) is not int or not 0 <= value <= maximum:
            invalid(key + " must be an integer in range")
    if body["receivedAt"] == 0:
        invalid("receivedAt is required")
    if body["receivedAt"] >= 100_000_000_000:
        invalid("receivedAt is in seconds, not milliseconds: use int(time.time()) or divide milliseconds by 1000")
    for key in ("mailFilters", "mailElements"):
        items = body.get(key, [])
        if not isinstance(items, list):
            invalid(key + " must be an array")
        if not items:
            invalid(key + " is required")
        if len(items) > 10:
            invalid(key + " has too many entries")
        for i, item in enumerate(items):
            at = "{}[{}].".format(key, i)
            if not isinstance(item, dict):
                invalid(at + "must be an object")
            if key == "mailFilters":
                for field in ("target", "comparator", "value", "regexReplaceAll"):
                    string(item, field, at)
                alternatives = item.get("orValues", [])
                if not isinstance(alternatives, list):
                    invalid(at + "orValues must be an array")
                if len(alternatives) > 10:
                    invalid("orValues has too many entries")
                for value in alternatives:
                    string({"orValues": value}, "orValues", at)
                enum(item.get("target", ""), ("from", "subject"), at + "target")
                enum(item.get("comparator", ""), ("includes", "excludes", "matches", "findByRegex"), at + "comparator")
                if not item.get("value", ""):
                    invalid("mailFilters[].value is required")
                boolean = "optional"
            else:
                for field in ("name", "target", "selector", "regex", "regexSource", "formatter"):
                    string(item, field, at)
                if not item.get("name", ""):
                    invalid("mailElements[].name is required")
                enum(item.get("target", ""), ("from", "subject", "body"), at + "target")
                if item["target"] == "body" and not (item.get("selector") or item.get("regex")):
                    invalid("mailElements[].selector or regex is required")
                enum(item.get("regexSource", ""), ("", "pretty", "raw"), at + "regexSource")
                enum(item.get("formatter", ""), ("", "text", "html", "link", "numeric"), at + "formatter")
                boolean = "penetrate"
            if type(item.get(boolean, False)) is not bool:
                invalid(at + boolean + " must be a boolean")
    if body["timeout"] == 0 or body["timeout"] > 3600:
        body["timeout"] = 3600
    return body
