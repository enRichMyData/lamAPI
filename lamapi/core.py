"""Core application facade used by both the CLI and the API server."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence

from lamapi.model.database import Database
from lamapi.model.params_validator import ParamsValidator
from lamapi.model.retrievers.bow_retriever import BOWRetriever
from lamapi.model.retrievers.column_analysis import ColumnAnalysis
from lamapi.model.retrievers.labels_retriever import LabelsRetriever
from lamapi.model.retrievers.literal_classifier import LiteralClassifier
from lamapi.model.retrievers.literals_retriever import LiteralsRetriever
from lamapi.model.retrievers.lookup_retriever import LookupRetriever
from lamapi.model.retrievers.ner_recognizer import NERRecognizer
from lamapi.model.retrievers.objects_retriever import ObjectsRetriever
from lamapi.model.retrievers.predicates_retriever import PredicatesRetriever
from lamapi.model.retrievers.sameas_retriever import SameasRetriever
from lamapi.model.retrievers.summary_retriever import SummaryRetriever
from lamapi.model.retrievers.types_retriever import TypesRetriever


class LamAPI:
    """High level wrapper around the different retrievers and utilities."""

    def __init__(self, database: Optional[Database] = None) -> None:
        self.database = database or Database()

        self.params_validator = ParamsValidator()
        self.types_retriever = TypesRetriever(self.database)
        self.objects_retriever = ObjectsRetriever(self.database)
        self.bow_retriever = BOWRetriever(self.database)
        self.predicates_retriever = PredicatesRetriever(self.database)
        self.labels_retriever = LabelsRetriever(self.database)
        self.literal_classifier = LiteralClassifier()
        self.literals_retriever = LiteralsRetriever(self.database)
        self.sameas_retriever = SameasRetriever(self.database)
        self.lookup_retriever = LookupRetriever(self.database)
        self.column_analysis_classifier = ColumnAnalysis()
        self.ner_recognizer = NERRecognizer()
        self.summary_retriever = SummaryRetriever(self.database)

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------
    def lookup(self, name: str, **kwargs: Any) -> List[Dict[str, Any]]:
        """Run the lookup retriever with the provided parameters."""

        return self.lookup_retriever.search(name=name, **kwargs)

    # ------------------------------------------------------------------
    # Types & Objects
    # ------------------------------------------------------------------
    def get_types(self, entities: Optional[Sequence[str]] = None, kg: str = "wikidata") -> Dict:
        return self.types_retriever.get_types_output(entities, kg)

    def get_objects(self, entities: Optional[Sequence[str]] = None, kg: str = "wikidata") -> Dict:
        return self.objects_retriever.get_objects_output(entities, kg)

    def get_literals(self, entities: Optional[Sequence[str]] = None, kg: str = "wikidata") -> Dict:
        return self.literals_retriever.get_literals_output(entities, kg)

    def get_sameas(self, entities: Optional[Sequence[str]] = None, kg: str = "wikidata") -> Dict:
        return self.sameas_retriever.get_sameas_output(entities, kg)

    def get_labels(
        self,
        entities: Optional[Sequence[str]] = None,
        kg: str = "wikidata",
        lang: Optional[str] = None,
        category: Optional[str] = None,
    ) -> Dict:
        return self.labels_retriever.get_labels_output(entities, kg, lang, category)

    # ------------------------------------------------------------------
    # Lookup helpers
    # ------------------------------------------------------------------
    def get_bow(
        self,
        text: str,
        entities: Optional[Sequence[str]] = None,
        kg: str = "wikidata",
    ) -> Dict[str, Dict[str, Any]]:
        return self.bow_retriever.get_bow_output(text, entities, kg)

    def get_predicates(
        self, entity_pairs: Optional[Iterable[Sequence[str]]] = None, kg: str = "wikidata"
    ) -> Dict:
        return self.predicates_retriever.get_predicates_output(entity_pairs, kg)

    # ------------------------------------------------------------------
    # Classifiers
    # ------------------------------------------------------------------
    def classify_literals(self, literals: Sequence[str]) -> Dict[str, Dict[str, Any]]:
        return self.literal_classifier.classifiy_literal(literals)

    def classify_columns(self, tables: Sequence[Sequence[Sequence[str]]]) -> List[Dict[str, Any]]:
        return self.column_analysis_classifier.classify_columns(tables)

    def recognize_entities(self, text_list: Sequence[str]) -> Dict[str, Any]:
        return self.ner_recognizer.recognize_entities(text_list)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    def get_objects_summary(
        self,
        entities: Optional[Sequence[str]] = None,
        kg: str = "wikidata",
        rank_order: str = "desc",
        k: int = 10,
    ) -> List[Dict[str, Any]]:
        return self.summary_retriever.get_objects_summary(entities, kg, rank_order, k)

    def get_literals_summary(
        self,
        entities: Optional[Sequence[str]] = None,
        kg: str = "wikidata",
        rank_order: str = "desc",
        k: int = 10,
    ) -> List[Dict[str, Any]]:
        return self.summary_retriever.get_literals_summary(entities, kg, rank_order, k)

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------
    def validate_token(self, token: str):
        return self.params_validator.validate_token(token)

    def validate_kg(self, kg: Optional[str]):
        return self.params_validator.validate_kg(self.database, kg)

    def validate_limit(self, limit: Optional[int]):
        return self.params_validator.validate_limit(limit)

    def validate_bool(self, value: Optional[str]):
        return self.params_validator.validate_bool(value)

    def validate_ner_type(self, value: Optional[str]):
        return self.params_validator.validate_NERtype(value)

    def parse_multi_values(self, values: Optional[Sequence[str]]) -> List[str]:
        return self.params_validator.parse_multi_values(values)
