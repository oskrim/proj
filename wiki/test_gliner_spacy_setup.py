#!/usr/bin/env python3
"""
Test script to verify GLiNER spaCy integration is working correctly.
"""

import spacy

def test_gliner_spacy_setup():
    """Test that gliner_spacy component can be added to a spaCy pipeline."""
    try:
        # Create a blank spaCy pipeline
        nlp = spacy.blank("en")
        
        # Add the gliner_spacy component with configuration
        custom_config = {
            "gliner_model": "urchade/gliner_mediumv2.1",
            "chunk_size": 250,
            "labels": ["PERSON", "ORGANIZATION", "LOCATION"],
            "style": "ent",
            "threshold": 0.75,
        }
        
        print("Adding gliner_spacy component to pipeline...")
        nlp.add_pipe("gliner_spacy", config=custom_config)
        
        print("✓ GLiNER spaCy component successfully registered!")
        print(f"Pipeline components: {nlp.pipe_names}")
        
        # Test with a simple sentence
        test_text = "Albert Einstein worked at Princeton University."
        print(f"\nTesting with text: '{test_text}'")
        doc = nlp(test_text)
        
        print("\nExtracted entities:")
        for ent in doc.ents:
            print(f"  - {ent.text} ({ent.label_})")
            
        return True
        
    except Exception as e:
        print(f"✗ Error setting up GLiNER spaCy: {e}")
        return False


if __name__ == "__main__":
    print("Testing GLiNER spaCy Integration")
    print("=" * 50)
    test_gliner_spacy_setup()