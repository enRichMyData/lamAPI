"""Command-line interface for LamAPI using jsonargparse."""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, Optional, Sequence

from dotenv import load_dotenv
from jsonargparse import ActionConfigFile, ArgumentParser

os.environ.setdefault("LAMAPI_RUNTIME", "local")

load_dotenv(override=True)

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
    return api.classify_columns(args.tables, model_type=args.model_type)


def _handle_ner(api: LamAPI, args) -> Any:
    return api.recognize_entities(args.texts)


def _handle_objects_summary(api: LamAPI, args) -> Any:
    return api.get_objects_summary(args.entities, kg=args.kg, rank_order=args.rank_order, k=args.k)


def _handle_literals_summary(api: LamAPI, args) -> Any:
    return api.get_literals_summary(
        args.entities, kg=args.kg, rank_order=args.rank_order, k=args.k
    )


def build_parser() -> tuple[ArgumentParser, dict[str, Any]]:
    parser = ArgumentParser(prog="lamapi", description="LamAPI command line interface")
    parser.add_argument("--config", action=ActionConfigFile, help="Path to a configuration file")

    subparsers = parser.add_subcommands()
    handlers: dict[str, Any] = {}

    lookup = ArgumentParser(prog="lamapi lookup", description="Run an entity lookup")
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
    subparsers.add_subcommand("lookup", lookup)
    handlers["lookup"] = _handle_lookup

    types_cmd = ArgumentParser(prog="lamapi types", description="Retrieve explicit types")
    types_cmd.add_argument("entities", nargs="+", help="Entity IDs")
    types_cmd.add_argument("--kg", default="wikidata")
    subparsers.add_subcommand("types", types_cmd)
    handlers["types"] = _handle_types

    objects_cmd = ArgumentParser(prog="lamapi objects", description="Retrieve objects")
    objects_cmd.add_argument("entities", nargs="+", help="Entity IDs")
    objects_cmd.add_argument("--kg", default="wikidata")
    subparsers.add_subcommand("objects", objects_cmd)
    handlers["objects"] = _handle_objects

    literals_cmd = ArgumentParser(prog="lamapi literals", description="Retrieve literals")
    literals_cmd.add_argument("entities", nargs="+", help="Entity IDs")
    literals_cmd.add_argument("--kg", default="wikidata")
    subparsers.add_subcommand("literals", literals_cmd)
    handlers["literals"] = _handle_literals

    sameas_cmd = ArgumentParser(prog="lamapi sameas", description="Retrieve sameAs URLs")
    sameas_cmd.add_argument("entities", nargs="+")
    sameas_cmd.add_argument("--kg", default="wikidata")
    subparsers.add_subcommand("sameas", sameas_cmd)
    handlers["sameas"] = _handle_sameas

    labels_cmd = ArgumentParser(prog="lamapi labels", description="Retrieve labels and aliases")
    labels_cmd.add_argument("entities", nargs="+")
    labels_cmd.add_argument("--kg", default="wikidata")
    labels_cmd.add_argument("--lang")
    labels_cmd.add_argument("--category")
    subparsers.add_subcommand("labels", labels_cmd)
    handlers["labels"] = _handle_labels

    bow_cmd = ArgumentParser(prog="lamapi bow", description="Compute Bag-of-Words similarities")
    bow_cmd.add_argument("text", help="Reference text")
    bow_cmd.add_argument("entities", nargs="+", help="Entity IDs")
    bow_cmd.add_argument("--kg", default="wikidata")
    subparsers.add_subcommand("bow", bow_cmd)
    handlers["bow"] = _handle_bow

    lit_class_cmd = ArgumentParser(
        prog="lamapi classify-literals", description="Classify literal strings"
    )
    lit_class_cmd.add_argument("literals", nargs="+", help="Literal values")
    subparsers.add_subcommand("classify-literals", lit_class_cmd)
    handlers["classify-literals"] = _handle_literals_classification

    col_cmd = ArgumentParser(
        prog="lamapi classify-columns",
        description="Classify table columns (provide as nested lists)",
    )
    col_cmd.add_argument("tables", nargs="+", type=json.loads, help="Tables in JSON list form")
    col_cmd.add_argument(
        "--model-type",
        choices=["fast", "accurate"],
        default="fast",
        help="Column classifier model to use",
    )
    subparsers.add_subcommand("classify-columns", col_cmd)
    handlers["classify-columns"] = _handle_columns

    ner_cmd = ArgumentParser(prog="lamapi ner", description="Run Named Entity Recognition")
    ner_cmd.add_argument("texts", nargs="+", help="Texts to analyse")
    subparsers.add_subcommand("ner", ner_cmd)
    handlers["ner"] = _handle_ner

    obj_summary_cmd = ArgumentParser(prog="lamapi objects-summary", description="Objects summary")
    obj_summary_cmd.add_argument("--entities", nargs="*", default=None)
    obj_summary_cmd.add_argument("--kg", default="wikidata")
    obj_summary_cmd.add_argument("--rank-order", choices=["asc", "desc"], default="desc")
    obj_summary_cmd.add_argument("-k", type=int, default=10)
    subparsers.add_subcommand("objects-summary", obj_summary_cmd)
    handlers["objects-summary"] = _handle_objects_summary

    lit_summary_cmd = ArgumentParser(
        prog="lamapi literals-summary", description="Literals summary"
    )
    lit_summary_cmd.add_argument("--entities", nargs="*", default=None)
    lit_summary_cmd.add_argument("--kg", default="wikidata")
    lit_summary_cmd.add_argument("--rank-order", choices=["asc", "desc"], default="desc")
    lit_summary_cmd.add_argument("-k", type=int, default=10)
    subparsers.add_subcommand("literals-summary", lit_summary_cmd)
    handlers["literals-summary"] = _handle_literals_summary

    return parser, handlers


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser, handlers = build_parser()
    args = parser.parse_args(argv)

    command = getattr(args, "subcommand", None)
    if not command:
        parser.print_help()
        parser.exit(2, "\nNo command provided.\n")

    handler = handlers.get(command)
    if handler is None:
        parser.error(f"Unknown command '{command}'")

    command_args = args[command]

    api = LamAPI()
    result = handler(api, command_args)
    if result is not None:
        _json_dump(result)


if __name__ == "__main__":  # pragma: no cover
    main()
