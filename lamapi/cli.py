"""Command-line interface for LamAPI using jsonargparse."""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, Optional, Sequence

from jsonargparse import ActionConfigFile, ArgumentParser

from lamapi import LamAPI


def _json_dump(data: Any) -> None:
    def _default(obj: Any) -> str:
        return str(obj)

    json.dump(data, sys.stdout, indent=2, ensure_ascii=False, default=_default)
    sys.stdout.write("\n")


def _handle_lookup(api: LamAPI, args) -> Any:
    kwargs: Dict[str, Any] = {
        "limit": args.limit,
        "kg": args.kg,
        "fuzzy": args.fuzzy,
        "kind": args.kind,
        "ner_type": args.ner_type,
        "language": args.language,
        "cache": args.cache,
        "soft_filtering": args.soft_filtering,
    }

    if args.types:
        kwargs["types"] = args.types
    if args.extended_types:
        kwargs["extended_types"] = args.extended_types
    if args.ids:
        kwargs["ids"] = " ".join(args.ids)
    if args.ner_type:
        kwargs["ner_type"] = args.ner_type

    return api.lookup(args.name, **kwargs)


def _handle_types(api: LamAPI, args) -> Any:
    return api.get_types(args.entities, kg=args.kg)


def _handle_objects(api: LamAPI, args) -> Any:
    return api.get_objects(args.entities, kg=args.kg)


def _handle_literals(api: LamAPI, args) -> Any:
    return api.get_literals(args.entities, kg=args.kg)


def _handle_sameas(api: LamAPI, args) -> Any:
    return api.get_sameas(args.entities, kg=args.kg)


def _handle_labels(api: LamAPI, args) -> Any:
    return api.get_labels(args.entities, kg=args.kg, lang=args.lang, category=args.category)


def _handle_bow(api: LamAPI, args) -> Any:
    return api.get_bow(args.text, args.entities, kg=args.kg)


def _handle_literals_classification(api: LamAPI, args) -> Any:
    return api.classify_literals(args.literals)


def _handle_columns(api: LamAPI, args) -> Any:
    return api.classify_columns(args.tables)


def _handle_ner(api: LamAPI, args) -> Any:
    return api.recognize_entities(args.texts)


def _handle_objects_summary(api: LamAPI, args) -> Any:
    return api.get_objects_summary(args.entities, kg=args.kg, rank_order=args.rank_order, k=args.k)


def _handle_literals_summary(api: LamAPI, args) -> Any:
    return api.get_literals_summary(
        args.entities, kg=args.kg, rank_order=args.rank_order, k=args.k
    )


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(prog="lamapi", description="LamAPI command line interface")
    parser.add_argument("--config", action=ActionConfigFile, help="Path to a configuration file")

    subparsers = parser.add_subcommands()

    lookup = subparsers.add_parser("lookup", help="Run an entity lookup")
    lookup.add_argument("--name", required=True, help="Query string")
    lookup.add_argument("--limit", type=int, default=10)
    lookup.add_argument("--kg", default="wikidata")
    lookup.add_argument("--fuzzy", type=bool, default=False)
    lookup.add_argument("--kind")
    lookup.add_argument("--ner-type")
    lookup.add_argument("--language")
    lookup.add_argument("--types", nargs="*", default=None)
    lookup.add_argument("--extended-types", nargs="*", default=None)
    lookup.add_argument("--ids", nargs="*", default=None)
    lookup.add_argument("--cache", type=bool, default=True)
    lookup.add_argument("--soft-filtering", type=bool, default=False)
    lookup.set_defaults(handler=_handle_lookup)

    types_cmd = subparsers.add_parser("types", help="Retrieve explicit types")
    types_cmd.add_argument("entities", nargs="+", help="Entity IDs")
    types_cmd.add_argument("--kg", default="wikidata")
    types_cmd.set_defaults(handler=_handle_types)

    objects_cmd = subparsers.add_parser("objects", help="Retrieve objects")
    objects_cmd.add_argument("entities", nargs="+", help="Entity IDs")
    objects_cmd.add_argument("--kg", default="wikidata")
    objects_cmd.set_defaults(handler=_handle_objects)

    literals_cmd = subparsers.add_parser("literals", help="Retrieve literals")
    literals_cmd.add_argument("entities", nargs="+", help="Entity IDs")
    literals_cmd.add_argument("--kg", default="wikidata")
    literals_cmd.set_defaults(handler=_handle_literals)

    sameas_cmd = subparsers.add_parser("sameas", help="Retrieve sameAs URLs")
    sameas_cmd.add_argument("entities", nargs="+")
    sameas_cmd.add_argument("--kg", default="wikidata")
    sameas_cmd.set_defaults(handler=_handle_sameas)

    labels_cmd = subparsers.add_parser("labels", help="Retrieve labels and aliases")
    labels_cmd.add_argument("entities", nargs="+")
    labels_cmd.add_argument("--kg", default="wikidata")
    labels_cmd.add_argument("--lang")
    labels_cmd.add_argument("--category")
    labels_cmd.set_defaults(handler=_handle_labels)

    bow_cmd = subparsers.add_parser("bow", help="Compute Bag-of-Words similarities")
    bow_cmd.add_argument("text", help="Reference text")
    bow_cmd.add_argument("entities", nargs="+", help="Entity IDs")
    bow_cmd.add_argument("--kg", default="wikidata")
    bow_cmd.set_defaults(handler=_handle_bow)

    lit_class_cmd = subparsers.add_parser("classify-literals", help="Classify literal strings")
    lit_class_cmd.add_argument("literals", nargs="+", help="Literal values")
    lit_class_cmd.set_defaults(handler=_handle_literals_classification)

    col_cmd = subparsers.add_parser(
        "classify-columns", help="Classify table columns (provide as nested lists)"
    )
    col_cmd.add_argument("tables", nargs="+", type=json.loads, help="Tables in JSON list form")
    col_cmd.set_defaults(handler=_handle_columns)

    ner_cmd = subparsers.add_parser("ner", help="Run Named Entity Recognition on text")
    ner_cmd.add_argument("texts", nargs="+", help="Texts to analyse")
    ner_cmd.set_defaults(handler=_handle_ner)

    obj_summary_cmd = subparsers.add_parser("objects-summary", help="Objects summary")
    obj_summary_cmd.add_argument("--entities", nargs="*", default=None)
    obj_summary_cmd.add_argument("--kg", default="wikidata")
    obj_summary_cmd.add_argument("--rank-order", choices=["asc", "desc"], default="desc")
    obj_summary_cmd.add_argument("-k", type=int, default=10)
    obj_summary_cmd.set_defaults(handler=_handle_objects_summary)

    lit_summary_cmd = subparsers.add_parser("literals-summary", help="Literals summary")
    lit_summary_cmd.add_argument("--entities", nargs="*", default=None)
    lit_summary_cmd.add_argument("--kg", default="wikidata")
    lit_summary_cmd.add_argument("--rank-order", choices=["asc", "desc"], default="desc")
    lit_summary_cmd.add_argument("-k", type=int, default=10)
    lit_summary_cmd.set_defaults(handler=_handle_literals_summary)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not hasattr(args, "handler"):
        parser.error("No command provided")

    api = LamAPI()
    result = args.handler(api, args)
    if result is not None:
        _json_dump(result)


if __name__ == "__main__":  # pragma: no cover
    main()
