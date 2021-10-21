# Multi-Page Dash App Plugin

A plugin to simplify creating multi-page Dash apps. This is a preview of functionality that will of Dash 2.1.

**[See the community announcement for details and discussion](https://community.plotly.com/t/introducing-dash-pages-dash-2-1-feature-preview/57775/2)**

### Background

The goal of this plugin is to remove as much boilerplate as possible when creating multi-page Dash apps.

This plugin allows users to simply place their layouts in `pages/` and call `dash.register_page` with the desired URL path of that page.

This plugin will automatically:
- Create the URL routing callback
- Add page information to `dash.page_registry` that can be used when creating navigation bars
- Set `validate_layout` accordingly so that you don't need to `suppress_callback_exceptions` for simple multi-page layouts
- Set the order of `dash.page_registry` based off `order`  and the filename
- Set `<title>` and `<meta description>` and their social media equivalents accordingly in the `index_string` of the HTML that is served on page-load
- Set a clientside callback to update the `<title>` as you navigate pages with `dcc.Link`
- Set the social media meta image accordingly based off of images available in assets

### Usage

**Option 1 - In this project**

Clone this repo and then run `python app.py`. `pages_plugin.py` is the functionality that will become part of the `dash` library. The `pages/` folder demonstrates examples of how to use `dash.register_page`.

**Option 2 - In your own projects**

1. Copy `pages_plugin.py` into your project folder. In the future, this will be part of `dash` and you won't need to copy this file.
2. In `app.py`, pass the plugin into `Dash`:

```python
import pages_plugin

app = Dash(__name__, plugins=[pages_plugin])
```
3. Create a folder called `pages/` and place your app layouts in files within that folder. Each file needs to:
- Define `layout`. This can be a variable or function that returns a component
- Call `dash.register_page(__name__)` to tell `pages_plugin` that this page should be part of the multi-page framework

For example:
`pages/historical_outlook.py`
```python
import dash
from dash import html

dash.register_page(__name__)

def layout():
    return html.Div('This page is the historical outlook')
```

`dash.register_page` will can accept various arguments to customize aspects about the page like `path` (the URL of the page), `title` (the browser tab's title of the page), and more. See the API reference below for details.

`pages/home.py`
```python
import dash
from dash import html

dash.register_page(
    __name__,
    path='/',
    title='Analytics App'
)

def layout():
    return html.Div('This is the home page')
```

4. Modify `app.layout` to display the URLs for page navigation and include the container that displays the page's content.
- `dash.page_registry`: The page URLs can be found in `dash.page_registry`. This is an `OrderedDict` with keys being the page's module name (e.g. `pages.historical_outlook`) and values being a dictionary containing keys `path`, `name`, `order`, `title`, `description`, `image`, and `layout`. This `page_registry` is populated from calling `dash.register_page` within `pages/`.
- `pages_plugin.page_container`: This component defines where the page's content will render on page navigation.

`app.py`

```python
import pages_plugin

app = Dash(__name__, plugins=[pages_plugin])

app.layout = html.Div([
    # Display the URLs by looping through `dash.page_registry`
    # In practice, this might be a `ddk.Header` or a `dbc.NavbarSimple`
    html.Div([dcc.Link(page['name'], href=page['path']) for page in dash.page_registry),
    
    html.Hr()
    
    # Set the container where the page content will be rendered into on page navigation
    pages_plugin.page_container
])
```

## Refrence

**`dash.register_page`**

```python
def register_page(
    module,
    path=None,
    name=None,
    order=None,
    title=None,
    description=None,
    image=None,
    layout=None,
    **kwargs
):
```

Assigns the variables to `dash.page_registry` as an `OrderedDict` 
(ordered by `order`). 

`dash.page_registry` is used by `pages_plugin` to set up the layouts as 
a multi-page Dash app. This includes the URL routing callbacks 
(using `dcc.Location`) and the HTML templates to include title,
meta description, and the meta description image.

`dash.page_registry` can also be used by Dash developers to create the 
page navigation links or by template authors.

- `module`:
   The module path where this page's `layout` is defined. Often `__name__`.

- `path`:
   URL Path, e.g. `/` or `/home-page`.
   If not supplied, will be inferred from `module`,
   e.g. `pages.weekly_analytics` to `/weekly-analytics`

- `name`:
   The name of the link.
   If not supplied, will be inferred from `module`,
   e.g. `pages.weekly_analytics` to `Weekly analytics`

- `order`:
   The order of the pages in `page_registry`.
   If not supplied, then the filename is used and the page with path `/` has
   order `0`

- `title`:
   The name of the page <title>. That is, what appears in the browser title.
   If not supplied, will use the supplied `name` or will be inferred by module,
   e.g. `pages.weekly_analytics` to `Weekly analytics`

- `description`:
   The <meta type="description"></meta>.
   If not supplied, then nothing is supplied.
    
- `image`:
   The meta description image used by social media platforms.
   If not supplied, then it looks for the following images in `assets/`:
    - A page specific image: `assets/<title>.<extension>` is used, e.g. `assets/weekly_analytics.png`
    - A generic app image at `assets/app.<extension>`
    - A logo at `assets/logo.<extension>`

- `redirect_from`:
   A list of paths that should redirect to this page
   For example: `redirect_from=['/v2', '/v3']`

- `layout`:
   The layout function or component for this page.
   If not supplied, then looks for `layout` from within the supplied `module`.

- `**kwargs`:
   Arbitrary keyword arguments that can be stored


`page_registry` stores the original property that was passed in under 
`supplied_<property>` and the coerced property under `<property>`. 
For example, if this was called:
```
register_page(
    'pages.historical_outlook',
    name='Our historical view',
    custom_key='custom value'
)
```
Then this will appear in `page_registry`:
```python
OrderedDict([
    (
        'pages.historical_outlook', 
        dict(
            module='pages.historical_outlook',
            
            supplied_path=None,
            path='/historical-outlook',
            
            supplied_name='Our historical view',
            name='Our historical view',
            
            supplied_title=None,
            title='Our historical view'
            
            supplied_description=None,
            description='Our historical view',
            
            supplied_order=None,
            order=1,
            
            supplied_layout=None,
            layout=<function pages.historical_outlook.layout>,
            
            custom_key='custom value'
        )
    ),
])
```
