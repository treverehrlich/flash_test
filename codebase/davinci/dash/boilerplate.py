from dash import html, dcc, Input, Output, no_update, clientside_callback, callback_context
from dash.exceptions import PreventUpdate
import dash_mantine_components as dmc
from time import sleep
from flask_login import current_user
import os

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

from copy import deepcopy

from collections import namedtuple
from davinci.dash.login import access_check
from davinci.services.auth import get_secret
from davinci.utils.global_config import SYSTEM
from dash import html, dcc, no_update
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State

import dash_bootstrap_components as dbc
from dash import Input, Output, State, html
import dash_mantine_components as dmc

from dash_iconify import DashIconify

BASE_URL = get_secret("DASH_PORTAL_BASE_URL", doppler=True)

LinkSpecs = namedtuple('Link', ['text', 'id', 'href', 'app_name'])

def navbar_accordion_items(in_app):
    """
    The navbar accordion items - these get dynamically updated
    depending on the user's permissions.

    :param in_app: Boolean to control whether the nav-links should
        refresh the page or just rerender certain components. This is
        important for the overarching multi-app structure. In general,
        this should be True.
    """

    # dbc.NavLink with external_link = in_app
    def AltNavLink(*args, **kwargs):
        return dbc.NavLink(*args, **({'external_link': in_app} | kwargs))

    def nav_accordion_item_builder(title, links):
        render_links = map(
            lambda link: dbc.NavItem(AltNavLink(link.text, id=link.id, href=link.href)),
            filter(
                lambda link: hasattr(current_user, 'app_access') and (link.app_name in current_user.app_access),
                links
            )
        )
        return dmc.AccordionItem(
            [
                dmc.AccordionControl(title),
                dmc.AccordionPanel(
                    list(render_links)
                ),
            ],
            value=title,
        )

    # login/logout/profile depending on user auth status.
    if hasattr(current_user, 'is_authenticated') and current_user.is_authenticated:
        user_profile_link = dbc.NavItem(AltNavLink(current_user.first_name, id="user-name-login", href="/profile", disabled=False, style={'color': '#5c5e62'}))
        login_link = dbc.NavItem(AltNavLink("Logout", id="user-action", href="/logout"))
    else:
        user_profile_link = dbc.NavItem(AltNavLink("", id="user-name-login", href="/profile", disabled=True))
        login_link = dbc.NavItem(AltNavLink("Login", id="user-action", href="/login"))

    # Main accordion
    main_accord = dmc.AccordionItem(
        [
            dmc.AccordionControl("Main"),
            dmc.AccordionPanel(
                [
                    dbc.NavItem(AltNavLink("Home", href="/home")),
                    user_profile_link,
                    login_link,
                ],
            ),
        ],
        value="Main",
    )

    # CT Accordion
    ct_accord = dmc.AccordionItem(
        [
            dmc.AccordionControl("Control Tower"),
            dmc.AccordionPanel(
                [
                    dbc.NavItem(AltNavLink("Integrated", id="integrated_ct_link", href="/")),
                    dbc.NavItem(AltNavLink("Distribution", id="dist_ct_link", href="/")),
                    dbc.NavItem(AltNavLink("Transportation", id="trans_ct_link", href="/")),
                ],
            ),
        ],
        value="Control Tower",
    )

    # Standard Apps Accordion
    standard_app_links = [
        LinkSpecs('Slot DC', 'slot_dc_link', '/', 'slot_dc'),
        LinkSpecs('Order Management', 'order_mgmt_link', '/', 'order_mgmt'),
        LinkSpecs('Volume Forecast', 'volume_forecast_link', '/volume_forecast/', 'volume_forecast'),
        LinkSpecs('KencoGPT', 'kenco_gpt_link', '/kencogpt/', 'kencogpt'),
    ]

    apps_accord = nav_accordion_item_builder('Standard Apps', standard_app_links)

    # Data Portals Accordion

    data_portal_links = [
        LinkSpecs('KLS', 'kls_data_portal_link', '/portal_kls/', 'portal_kls'),
        LinkSpecs('LMS', 'lms_data_portal_link', '/portal_lms/', 'portal_lms'),
    ]

    data_portals_accord = nav_accordion_item_builder('Data Portals', data_portal_links)

    # Custom Apps Accordion

    custom_apps_links = [
        LinkSpecs('S&OP Genie', 's_and_op_link', '/s_and_op_genie/', 'chervon_sop_genie'),
        LinkSpecs('Pick Path', 'pick_path_link', '/pick_path/', 'bluebuff_pick_path'),
        LinkSpecs('AVRL Pricing', 'avrl_pricing_link', '/avrl_pricing/', 'AVRL Pricing'),
    ]

    custom_apps_accord = nav_accordion_item_builder('Custom Apps', custom_apps_links)

    # Custom Apps Accordion
    admin_accordion = dmc.AccordionItem(
        [
            dmc.AccordionControl("Admin"),
            dmc.AccordionPanel(
                [
                    dbc.NavItem(AltNavLink("Auth Controller", id="auth_control_link", href="/auth_controller")),
                ],
            ),
        ],
        value="Admin",
    )

    # Always render the Main accordion...
    close_button = dmc.MediaQuery(
        dmc.ActionIcon(DashIconify(icon='octicon:x-16'), id='navbar-close-button', variant='transparent', style={'color': 'black', 'display': 'none'}),
        smallerThan="601",
        styles={'display': 'none'}
    )

    accordion_items = [
        close_button,
        html.Br(),
        main_accord,
    ]

    # Conditionally render the rest...
    if hasattr(current_user, 'groups'):
        # Only Dev and Superuser see these
        if current_user.groups & {'superuser'}:
            accordion_items.append(ct_accord)
        if current_user.accordions & {'standard_apps'}:
            accordion_items.append(apps_accord)
        # Restrict the next two based on Cognito group
        # (Dev and Superuser inherit all groups)
        if current_user.accordions & {'data_portals'}:
            accordion_items.append(data_portals_accord)
        if current_user.accordions & {'custom_apps'}:
            accordion_items.append(custom_apps_accord)
        if current_user.accordions & {'admin'}:
            accordion_items.append(admin_accordion)

    accordion_items.append(html.Br())
    # Logos
    DAVINCI_LOGO = "/assets/davinci_logo.png"
    KENCO_LOGO = "/assets/kenco_logo.png"
    accordion_items.append(dmc.Grid([
        dbc.Col(html.Img(src=DAVINCI_LOGO, style={'width': '70%', 'paddingTop': '10px', 'paddingLeft': '35px'}), sm=6, md=12, lg=12, xl=12),
        dbc.Col(html.Img(src=KENCO_LOGO, style={'width': '70%', 'paddingTop': '10px', 'paddingLeft': '20px'}), sm=6, md=12, lg=12, xl=12)
    ]))
    return accordion_items

# Preprend 'grid_' to each child's ID.
# This facilitates the mobile navigation menu.
def recursive_prepend_grid_prefix(l):
    if type(l) == str:
        return
    if hasattr(l, '__iter__'):
        [recursive_prepend_grid_prefix(i) for i in l if len(i) >= 1]
    if hasattr(l, 'children'):
        recursive_prepend_grid_prefix(l.children)
    if hasattr(l, 'id'):
        l.id = 'grid_' + l.id

def make_grid_children(children):
    grid_children = deepcopy(children)
    recursive_prepend_grid_prefix(grid_children)
    return grid_children

# Create the sidebar
# This was taken from https://dash-building-blocks.com/navbars, see the Advanced Navbar
def sidebarNav(
    children, gridChildren, border=True, collapse=True
):
    navStyle = {"margin": "0", "position": "sticky"}
    navClass = "collapsible" if collapse else "" 
    for i in range(len(gridChildren)):
        gridChildren[i] = html.Div(gridChildren[i], style={'flexShrink':1})
    gridChildren.insert(0, html.Div(html.Div(className='fa-solid fa-thumbtack', id='navThumb', n_clicks=0), id='navPin'))

    for i in range(len(children)):
        children[i] = html.Div(
            children[i],
        )
    if border:
        navStyle["borderRight"] = "1pt solid black"

    navStyle = {**navStyle, 
        "flexDirection": "column",
        # "height": "70rem",
        'width': '0vw', #hidden at first
        'transition': 'all 0s',
        'zIndex': 50,
        'height': '100vh',
    }

    render_hamburger = dmc.Affix(
        dmc.MediaQuery(
            dmc.ActionIcon(DashIconify(icon='ci:hamburger-md'), id='navbar-render-button', size='lg', style={'color': 'black'}),
        smallerThan="601",
        styles={'display': 'none'}
        ),
        position={'top': 7, 'left': 20},
    )

    nav = [
            dmc.MediaQuery(
                dmc.Navbar(
                    children=[render_hamburger] + children,
                    hidden=True,
                    hiddenBreakpoint=600,
                    style=navStyle,
                    id="navbar",
                    className=navClass,
                ),
                smallerThan="601",
                styles={"display": "none"},
            ),
            dmc.MediaQuery(
                    html.Div(dmc.Grid([
                        dmc.Col(
                            dmc.ActionIcon(DashIconify(icon='ci:hamburger-md'), size='lg', style={'color': 'black'}),
                            style={
                                "display": "flex",
                                "justifyContent": "center",
                                "alignItems": "center",
                            },
                            span=10,
                            offset=1,
                        ),
                    ],
                    style={
                        "width": "100vw",
                        "margin": 0,
                        'background': 'white'
                    },
                ),
                id="mobileNav",
                style={'position':'sticky'}),
                largerThan="601",
                styles={"display": "none"},
            ),
        dmc.Drawer(gridChildren, title='', id='mobileMenu', size='full', position='top')
        ]
    return nav


def create_standard_layout(children=None, in_app=True, use_loader=True, loader_color='#1899aa'):
    """
    Create the standardized layout including navbar and
    necessities for loader and login-auth.

    :param children: Dash list of children to pass in. This is where your app should go.
    :param in_app: Set to True if this isn't the login/home app. Otherwise False.
    :param use_loader: Include HTML for nice loader functionality
    """

    # Dark mode toggler (not in use yet)
    def darkModeToggle():
        return html.Div(
            dmc.Switch(
                offLabel=DashIconify(icon="radix-icons:moon", width=20),
                onLabel=DashIconify(icon="radix-icons:sun", width=20),
                size="sm",
                id='themeSwitch',
                sx={'display':'flex', 'paddingTop':'2px', 'paddingBottom': '2px'},
                persistence=True,
            ),
        id='themeSwitchHolder')

    # Preprend 'grid_' to each child's ID.
    # This facilitates the mobile navigation menu.
    def recursive_prepend_grid_prefix(l):
        if type(l) == str:
            return
        if hasattr(l, '__iter__'):
            [recursive_prepend_grid_prefix(i) for i in l if len(i) >= 1]
        if hasattr(l, 'children'):
            recursive_prepend_grid_prefix(l.children)
        if hasattr(l, 'id'):
            l.id = 'grid_' + l.id

    def make_grid_children(children):
        grid_children = deepcopy(children)
        recursive_prepend_grid_prefix(grid_children)
        return grid_children

    # Create the sidebar
    # This was taken from https://dash-building-blocks.com/navbars, see the Advanced Navbar
    def sidebarNav(
        children, gridChildren, title="custom menu", border=True, collapse=True
    ):
        navStyle = {"margin": "0", "position": "sticky"}
        navClass = "collapsible" if collapse else "" 
        for i in range(len(gridChildren)):
            gridChildren[i] = html.Div(gridChildren[i], style={'flexShrink':1})
        gridChildren.insert(0, html.Div(html.Div(className='fa-solid fa-thumbtack', id='navThumb', n_clicks=0), id='navPin'))

        for i in range(len(children)):
            children[i] = html.Div(
                children[i],
            )
        if border:
            navStyle["borderRight"] = "1pt solid black"

        navStyle = {**navStyle, 
            "flexDirection": "column",
            # "height": "70rem",
            'width': '0vw', #hidden at first
            'transition': 'all 0s',
            'zIndex': 50,
            'height': '100vh',
        }
        nav = [
                dmc.MediaQuery(
                    dmc.Navbar(
                        children=[
                            dmc.Affix(
                                dmc.ActionIcon(DashIconify(icon='ci:hamburger-md'), id='navbar-render-button', size='lg', style={'color': 'black'}),
                                position={'top': 7, 'left': 20},
                            ),
                        ] + children,
                        style=navStyle,
                        id="navbar",
                        className=navClass,
                    ),
                    smallerThan="601",
                    styles={"display": "none"},
                ),
                dmc.MediaQuery(
                        html.Div(dmc.Grid([
                            dmc.Col(
                                title,
                                style={
                                    "display": "flex",
                                    "justifyContent": "center",
                                    "alignItems": "center",
                                },
                                span=10,
                                offset=1,
                            ),
                        ],
                        style={
                            "width": "100vw",
                            "margin": 0,
                            'background': 'white'
                        },
                    ),
                    id="mobileNav",
                    style={'position':'sticky'}),
                    largerThan="601",
                    styles={"display": "none"},
                ),
            dmc.Drawer(gridChildren, title='', id='mobileMenu', size='full', position='top', style={'background': 'white'})
            ]
        return nav

    # Children that fit in the accordion before creating sidebar
    nav_children = [
        # dmc.Avatar(style={"display": "block", 'width':'100%',}, color='red'),
        dmc.Accordion(id='nav-accordion-content', children=navbar_accordion_items(in_app=in_app)),
    ]

    # Create navbar
    nav = html.Div(
        id='nav-holder',
        children=sidebarNav(
            nav_children,
            make_grid_children(nav_children),
            False,
            True,
        )
    )

    # Dummy container to center the loader.
    starting_children = dbc.Container(
        dbc.Row(
            html.Div(),
            justify='center', align='center', className='h-50'
        ),
        style={'height': '100vh', 'display': 'grid'}
    )
    
    # Create the page container with the loader
    refresh = True if in_app else 'callback-nav'
    page_content = html.Div(id='page-content-wrapper', children=(
            dcc.Loading(
                children=html.Div(id='page-content', children=starting_children),
                fullscreen=False,
                type='cube',
                color=loader_color,
                style={"backgroundColor":"transparent"}
    ))) if use_loader else html.Div(id='page-content-wrapper', children=html.Div(id='page-content', children=starting_children))

    # Create the final layout with all providers/wrappers, navbar, and loading div
    layout = dmc.MantineProvider(
        dmc.NotificationsProvider([
        html.Div([
            html.Div(id='overall-notification'),
            nav,
            html.Div(
                [
                    dcc.Store(id='session', storage_type='session'),
                    dcc.Store(id='localstorage', storage_type='local'),
                    # header,
                    html.Div(
                        id='pre-page-content-div',
                        children=page_content
                    ),
                    dcc.Location(id="base-url", refresh=refresh),
                ],
                className="content",
                id='content',
                style={"flexShrink": "1", "flexGrow": "1"},
            ),
        ],
        id='overallContainer', style={},
        ),
        # darkModeToggle(),
        ]
        ),
        # darkModeToggle(),
        id='themeHolder',
        theme={"colorScheme": "light"},
        withNormalizeCSS=True,
        withGlobalStyles=True,
        withCSSVariables=True
    )
    return layout

def _slow_down_wrapper(f):
    """
    Wrapper that artificially adds load-time. This seems dumb,
    but makes the UX better by not flashing the loader for too
    short a time.
    """
    def _f(*args, **kwargs):
        res = f(*args, **kwargs)
        sleep(1.5)
        return res
    return _f

def create_standard_callbacks(app, render_layout=None, app_name=None, in_app=True):
    """
    Create all of the standardized callbacks that go with the standard layout.

    :param app: the Dash app object.
    :param render_layout: a function that renders your app's layout
    :param groups: an optional SET of groups indicating who has access to the app.
        This should reference cognito groups. IF NOT SET, THE APP WILL NOT HAVE AUTH
        RESTRICTIONS
    :param in_app: whether this is the login-page or another app. Leave this set to True
        for your apps.
    """

    if os.path.exists('assets/favicon.ico'):
        app.head = [html.Link(rel='icon', href="assets/favicon.ico")]

    # Login handling
    if render_layout and app_name:
        slower_render = _slow_down_wrapper(render_layout)
        @app.callback(
            [Output('base-url', 'href', allow_duplicate=True),
            Output('page-content', 'children', allow_duplicate=True),
            Output('page-content-wrapper', 'children', allow_duplicate=True)],
            Input('base-url', 'pathname'),
            State('base-url', 'search'),
            State('base-url', 'hash'),
        )
        def login_handler(url, search, hash):
            # If on a developer system, allow render_layout()
            # to call unhindered.
            # Otherwise, check user auth.
            if not SYSTEM or access_check(current_user, app_name):
                return no_update, html.Div(''), html.Div(id='page-content', children=slower_render())
            # Redirect to login to get cookie
            reroute_page = f"{BASE_URL}/home" if (hasattr(current_user, 'is_authenticated') and current_user.is_authenticated) else f"{BASE_URL}/login"
            callback_context.response.set_cookie('first_page_attempt_pathname', bytes(url, 'utf-8')) 
            callback_context.response.set_cookie('first_page_attempt_search', bytes(search, 'utf-8')) 
            callback_context.response.set_cookie('first_page_attempt_hash', bytes(hash, 'utf-8')) 
            return reroute_page, no_update, no_update


    # Render navbar accordions dynamically based on auth.
    @app.callback(
        Output("nav-holder", "children", allow_duplicate=True),
        [Input("page-content", "children")],
        prevent_initial_call=True
    )
    def update_navlayout(content):
        nav_children = [
            dmc.Accordion(id='nav-accordion-content', children=navbar_accordion_items(in_app=in_app)),
        ]
        return sidebarNav(
                    nav_children,
                    make_grid_children(nav_children),
                    False,
                    True,
                )

    @app.callback(
        Output('navbar', 'style', allow_duplicate=True),
        Output('navbar-render-button', 'style', allow_duplicate=True),
        Output('navbar-close-button', 'style', allow_duplicate=True),
        Output('content', 'style', allow_duplicate=True),
        Output('overallContainer', 'style', allow_duplicate=True),
        Input('navbar-render-button', 'n_clicks'),
        State('navbar', 'style'),
        State('navbar-render-button', 'style'),
        State('navbar-close-button', 'style'),
        State('content', 'style'),
        State('overallContainer', 'style'),
        prevent_initial_call=True
    )
    def nav_open(click, curr_style, curr_nav_style, curr_nav_close_style, curr_content_style, overall_style):
        if not click:
            raise PreventUpdate
        curr_style['width'] = '12vw'
        curr_nav_style |= {'display': 'none'}
        curr_nav_close_style |= {'display': 'flex'}
        curr_content_style |= {'filter': 'blur(5px)'}
        overall_style |= {'background': '#DDD'}
        return curr_style, curr_nav_style, curr_nav_close_style, curr_content_style, overall_style
    
    @app.callback(
        Output('navbar', 'style', allow_duplicate=True),
        Output('navbar-render-button', 'style', allow_duplicate=True),
        Output('navbar-close-button', 'style', allow_duplicate=True),
        Output('content', 'style', allow_duplicate=True),
        Output('overallContainer', 'style', allow_duplicate=True),
        Input('navbar-close-button', 'n_clicks'),
        State('navbar', 'style'),
        State('navbar-render-button', 'style'),
        State('navbar-close-button', 'style'),
        State('content', 'style'),
        State('overallContainer', 'style'),
        prevent_initial_call=True
    )
    def nav_close(click, curr_style, curr_nav_style, curr_nav_close_style, curr_content_style, overall_style):
        if not click:
            raise PreventUpdate
        if curr_style['width'] == '0vw':
            return (no_update, ) * 5
        curr_style['width'] = '0vw'
        curr_nav_style |= {'display': 'inline-block', 'color': 'black'}
        curr_nav_close_style |= {'display': 'none'}
        curr_content_style |= {'filter': 'none'}
        overall_style |= {'background': 'none'}
        return curr_style, curr_nav_style, curr_nav_close_style, curr_content_style, overall_style

    clientside_callback(
        """
            function(id) {
                document.getElementById(id).addEventListener("click", function(event) {
                    document.getElementById('navbar-close-button').click()
                });
                return window.dash_clientside.no_update       
            }
        """,
        Output("navbar-close-button", "id"),
        Input("content", "id")
    )

    # Render profile link dynamically based on auth.
    @app.callback(
        [Output("user-name-login", "children", allow_duplicate=True),
        Output("user-action", "children", allow_duplicate=True),
        Output("user-action", "href", allow_duplicate=True),
        Output("grid_user-name-login", "children", allow_duplicate=True),
        Output("grid_user-action", "children", allow_duplicate=True),
        Output("grid_user-action", "href", allow_duplicate=True),],
        [Input("page-content", "children")]
    )
    def profile_link(content):
        """
        returns a navbar link to the user profile if the user is authenticated
        """
        if current_user.is_authenticated:
            # return html.Div(current_user.first_name)
            return current_user.first_name, "Logout", "/logout", current_user.first_name, "Logout", "/logout"
        else:
            return "", "Login", "/login", "", "Login", "/login"

    # Hamburger for mobile view
    @app.callback(
        Output('mobileMenu','opened'),
        Input('mobileNav','n_clicks')
    )
    def openMenu(n):
        if n:
            return True
        return no_update

    return (
        update_navlayout,
        profile_link
    )
