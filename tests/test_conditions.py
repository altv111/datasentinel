from datasentinel import conditions


def test_parse_properties_ignores_comments_and_blank_lines():
    text = """
# comment

IS_EMPTY: SELECT COUNT(*) = 0 AS passed FROM __LHS__
IS_SUBSET_OF=SELECT COUNT(*) = 0 AS passed FROM __RHS__
"""
    parsed = conditions._parse_properties(text)
    assert parsed["IS_EMPTY"].startswith("SELECT COUNT(*)")
    assert parsed["IS_SUBSET_OF"].startswith("SELECT COUNT(*)")


def test_load_conditions_user_overrides_builtin(tmp_path, monkeypatch):
    builtin_path = tmp_path / "conditions.properties"
    builtin_path.write_text("IS_EMPTY: SELECT 1 AS passed\n")
    user_path = tmp_path / "user_conditions.properties"
    user_path.write_text("IS_EMPTY: SELECT 0 AS passed\n")

    merged = conditions.load_conditions(
        base_path=str(builtin_path),
        user_path=str(user_path),
    )
    assert merged["IS_EMPTY"].strip() == "SELECT 0 AS passed"
