from ednews.processors import pressdemocrat


def test_local_news_tag_exact_term():
    entry = {"tags": [{"term": "Local News"}]}
    assert pressdemocrat._entry_has_local_news_category(entry)


def test_local_news_tag_bracketed():
    entry = {"tags": [{"term": "[Local News]"}]}
    assert pressdemocrat._entry_has_local_news_category(entry)


def test_local_news_tag_case_insensitive():
    entry = {"tags": [{"term": "local NEWS"}]}
    assert pressdemocrat._entry_has_local_news_category(entry)


def test_local_news_tag_substring():
    entry = {"tags": [{"term": "Some / Local News / Section"}]}
    assert pressdemocrat._entry_has_local_news_category(entry)


def test_missing_tags_uses_category_field():
    entry = {"category": "Local News"}
    assert pressdemocrat._entry_has_local_news_category(entry)


def test_missing_tags_and_category_returns_false():
    entry = {"title": "No category here"}
    assert not pressdemocrat._entry_has_local_news_category(entry)


def test_non_dict_tag_entries_are_handled():
    entry = {"tags": ["Local News", 123, None, {"term": "Other"}]} 
    # string tag should be ignored; dict entry with 'term' doesn't match; overall False
    assert not pressdemocrat._entry_has_local_news_category(entry)


def test_malformed_tag_dicts_are_ignored():
    entry = {"tags": [{}, {"notterm": "Local News"}, {"term": "local news"}]}
    assert pressdemocrat._entry_has_local_news_category(entry)
