#!/usr/bin/env python3
"""
Test script to verify the complete GLiNER + GLiREL pipeline.
"""

import spacy
import glirel  # Import for side effects

def test_complete_extraction_pipeline():
    """Test the complete entity and relation extraction pipeline."""
    try:
        # Create a blank spaCy pipeline
        nlp = spacy.blank("en")

        # Add GLiNER for entity extraction
        gliner_config = {
            "gliner_model": "urchade/gliner_mediumv2.1",
            "chunk_size": 250,
            "labels": ["PERSON", "ORGANIZATION", "LOCATION"],
            "style": "ent",
            "threshold": 0.75,
        }

        print("Adding gliner_spacy component...")
        nlp.add_pipe("gliner_spacy", config=gliner_config)

        # Add GLiREL for relation extraction
        print("Adding glirel component...")
        nlp.add_pipe("glirel", after="gliner_spacy")

        print("\n✓ Pipeline successfully configured!")
        print(f"Components: {nlp.pipe_names}")

        # Test with sample text
        test_text = "Albert Einstein was born in Germany and worked at Princeton University."
        relation_types = ["born_in", "worked_for", "located_in"]

        print(f"\nProcessing text: '{test_text}'")
        print(f"Looking for relations: {relation_types}")

        # Process with relation labels
        doc = nlp(test_text, component_cfg={"glirel": {"labels": relation_types}})

        # Extract entities
        print("\nExtracted Entities:")
        for ent in doc.ents:
            print(f"  - {ent.text} ({ent.label_})")

        # Extract relations
        print("\nExtracted Relations:")
        if hasattr(doc._, 'relations') and doc._.relations:
            for rel in doc._.relations:
                if rel.get('score', 0) >= 0.75:
                    head = " ".join(rel['head_text'])
                    tail = " ".join(rel['tail_text'])
                    print(f"  - {head} --[{rel['label']}]--> {tail} (score: {rel['score']:.2f})")
        else:
            print("  - No relations found")

        return True

    except Exception as e:
        print(f"\n✗ Error in pipeline: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("Testing Complete GLiNER + GLiREL Pipeline")
    print("=" * 50)
    test_complete_extraction_pipeline()
