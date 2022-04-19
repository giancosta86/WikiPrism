from functools import wraps
from xml.sax import SAXParseException, parseString

from info.gianlucacosta.eos.core.functional import Producer
from pytest import raises

from info.gianlucacosta.wikiprism.page import Page
from info.gianlucacosta.wikiprism.page.sax import WikiContentHandler, WikiSaxCanceledException


def wiki(source: str):
    def decorator(test_function: Producer[list[Page]]):
        @wraps(test_function)
        def wrapper():
            page_list = []

            content_handler = WikiContentHandler(
                on_page_extracted=page_list.append,
                continuation_provider=lambda: True,
            )

            parseString(source, content_handler)

            expected_page_list = test_function()

            assert page_list == expected_page_list

        return wrapper

    return decorator


@wiki("<hello>world</hello>")
def test_when_string_with_other_tags():
    return []


@wiki(
    """
    <wiki>
        <sometag>Hola!</sometag>

        <page>
            <title>Alpha</title>
            <someprop>A</someprop>
            <text>This is the text!</text>
        </page>

        <someclosingtag>Z</someclosingtag>
    </wiki>
    """
)
def test_when_string_with_one_page():
    return [Page(title="Alpha", text="This is the text!")]


@wiki(
    """
    <wiki>
        <sometag>Hola!</sometag>

        <page>
            <title>Alpha</title>
            <someprop>A</someprop>
            <text>First text</text>
        </page>

        <page>
            <title>Beta</title>
            <someprop>B</someprop>
            <text>Second text</text>
        </page>

        <page>
            <title>Gamma</title>
            <text>Third text</text>
            <yetanotherprop>C</yetanotherprop>
        </page>

        <someclosingtag>Z</someclosingtag>
    </wiki>
    """
)
def test_when_string_with_multiple_pages():
    return [
        Page(title="Alpha", text="First text"),
        Page(title="Beta", text="Second text"),
        Page(title="Gamma", text="Third text"),
    ]


@wiki(
    """
    <wiki>
        <sometag>Hola!</sometag>

        <page>
            <title>Alpha</title>
            <someprop>A</someprop>
            <text>First text</text>
        </page>

        <page>
            <someprop>B</someprop>
            <text>THIS PAGE HAS NO TITLE!</text>
        </page>

        <page>
            <title>Gamma</title>
            <text>Third text</text>
            <yetanotherprop>C</yetanotherprop>
        </page>

        <someclosingtag>Z</someclosingtag>
    </wiki>
    """
)
def test_when_page_with_missing_title():
    return [
        Page(title="Alpha", text="First text"),
        Page(title="Gamma", text="Third text"),
    ]


@wiki(
    """
    <wiki>
        <sometag>Hola!</sometag>

        <page>
            <title>Alpha</title>
            <someprop>A</someprop>
            <text>First text</text>
        </page>

        <page>
            <title>Beta</title>
            <someprop>THIS PAGE HAS NO TEXT!</someprop>
        </page>

        <page>
            <title>Gamma</title>
            <text>Third text</text>
            <yetanotherprop>C</yetanotherprop>
        </page>

        <someclosingtag>Z</someclosingtag>
    </wiki>
    """
)
def test_when_string_with_missing_text():
    return [
        Page(title="Alpha", text="First text"),
        Page(title="Gamma", text="Third text"),
    ]


def test_when_canceled():
    source = """
    <wiki>
        <sometag>Hola!</sometag>

        <page>
            <title>Alpha</title>
            <someprop>A</someprop>
            <text>First text</text>
        </page>

        <page>
            <title>Beta</title>
            <someprop>THIS PAGE HAS NO TEXT!</someprop>
        </page>

        <page>
            <title>Gamma</title>
            <text>Third text</text>
            <yetanotherprop>C</yetanotherprop>
        </page>

        <someclosingtag>Z</someclosingtag>
    </wiki>
    """

    page_list = []

    content_handler = WikiContentHandler(
        on_page_extracted=page_list.append,
        continuation_provider=lambda: not page_list,
    )

    try:
        parseString(source, content_handler)
        assert False
    except WikiSaxCanceledException:
        assert page_list == [Page(title="Alpha", text="First text")]


def test_invalid_wiki():
    page_list: list[Page] = []

    content_handler = WikiContentHandler(
        on_page_extracted=page_list.append,
        continuation_provider=lambda: True,
    )

    with raises(SAXParseException):
        parseString("INVALID_XML", content_handler)

    assert not page_list
