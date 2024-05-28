from davinci.services.auth import get_secret
import dash_mantine_components as dmc
from shortuuid import uuid
from serving import app
import time
import requests
import json
import pandas as pd
import numpy as np
from dash_iconify import DashIconify

from dash import Dash, html, Input, Output, callback, State
import dash_cytoscape as cyto
from davinci.services.auth import get_cognito_client, get_secret
from davinci.dash.login import inject_user_group_redis, get_redis_db, has_access
from davinci.utils.global_config import SYSTEM

def list_cognito_groups_for_user(user):
    """
    List user-groups for user. Doesn't require token
    since it uses an admin account in the background.
    
    :param user: username string
    :type user: str
    :return: list of user groups
    :rtype: list[str]
    """
    try:
        client = get_cognito_client()
        response = client.admin_list_groups_for_user(
            Username=user,
            UserPoolId=get_secret('AWS_COGNITO_USER_POOL_ID', doppler=True),
            Limit=59,
        )
        groups = response['Groups']
        group_list = [d['GroupName'] for d in groups]
        return group_list
    except:
        return []

def add_cognito_group_user(user, group):
    """
    List user-groups for user. Doesn't require token
    since it uses an admin account in the background.
    
    :param user: username string
    :type user: str
    :return: list of user groups
    :rtype: list[str]
    """
    try:
        client = get_cognito_client()
        response = client.admin_add_user_to_group(
            Username=user,
            GroupName=group,
            UserPoolId=get_secret('AWS_COGNITO_USER_POOL_ID', doppler=True),
        )
        return response
    except:
        return []

def remove_cognito_group_user(user, group):
    """
    List user-groups for user. Doesn't require token
    since it uses an admin account in the background.
    
    :param user: username string
    :type user: str
    :return: list of user groups
    :rtype: list[str]
    """
    try:
        client = get_cognito_client()
        response = client.admin_remove_user_from_group(
            Username=user,
            GroupName=group,
            UserPoolId=get_secret('AWS_COGNITO_USER_POOL_ID', doppler=True),
        )
        return response
    except Exception as e:
        print(e)
        return []

def list_all_user_groups():
    """
    List user-groups for user. Doesn't require token
    since it uses an admin account in the background.
    
    :param user: username string
    :type user: str
    :return: list of user groups
    :rtype: list[str]
    """
    try:
        client = get_cognito_client()
        response = client.list_groups(
            UserPoolId=get_secret('AWS_COGNITO_USER_POOL_ID', doppler=True),
        )
        groups = list(map(lambda x: x['GroupName'], response['Groups']))
        desc = list(map(lambda x: x['Description'], response['Groups']))
        return dict(zip(groups, desc))
    except Exception as e:
        print(e)
        return []



def get_all_users():
    """
    List user-groups for user. Doesn't require token
    since it uses an admin account in the background.
    
    :param user: username string
    :type user: str
    :return: list of user groups
    :rtype: list[str]
    """
    try:
        client = get_cognito_client()
        response = client.list_users(
            UserPoolId=get_secret('AWS_COGNITO_USER_POOL_ID', doppler=True),
        )
        return response
    except:
        return []

def load_json(st):
    if 'http' in st:
        return requests.get(st).json()
    else:
        with open(st, 'rb') as f:
            x = json.load(f)
        return x

# Load Data
stylesheet = load_json('https://js.cytoscape.org/demos/colajs-graph/cy-style.json')
users = get_all_users()
def get_value_from_key(l, k):
    try:
        return list(filter(lambda i: i['Name'] == k, l))[0]['Value']
    except IndexError:
        return np.nan

def make_user_df(users):
    users = users['Users']
    usernames = list(map(lambda x: x['Username'], users))
    firstnames = list(map(lambda x: get_value_from_key(x['Attributes'], 'name'), users))
    email = list(map(lambda x: get_value_from_key(x['Attributes'], 'email'), users))
    email_verified = list(map(lambda x: get_value_from_key(x['Attributes'], 'email_verified'), users))
    user_enabled = list(map(lambda x: x['Enabled'], users))
    user_created = list(map(lambda x: x['UserCreateDate'], users))
    user_status = list(map(lambda x: x['UserStatus'], users))
    return pd.DataFrame({
        'Username': usernames,
        'FirstName': firstnames,
        'Email': email,
        'EmailVerified': email_verified,
        'UserCreated': user_created,
        'UserEnabled': user_enabled,
        'UserStatus': user_status,
    })

user_df = make_user_df(users)
styles = {
    'container': {
        'position': 'fixed',
        'display': 'flex',
        'flex-direction': 'column',
        'height': '100vh',
        'width': '100%'
    },
    'cy-container': {
        'flex': '1',
        'position': 'relative'
    },
    'cytoscape': {
        'position': 'absolute',
        'width': '100%',
        'height': '100%',
        'z-index': 999
    }
}

def root_id_string(names):
    return ", ".join(map(lambda x: f'[id = "{x}"]', names))

def create_app_auth_structure():
    class Group:
        def __init__(self, name, children=None):
            self.name = name
            self.children = children if children else []
            self.access = False
            self.inherited = False
        
        def __repr__(self):
            return f"{self.name} - {self.children}"

    group_struct = json.load(open("pages/auth_pages/group_inheritance.json", 'r'))
    GROUPS = {k['name']: Group(k['name']) for k in group_struct['groups']}
    inheritance = {k['name']: k['inherits'] for k in group_struct['groups']}
    for group in GROUPS:
        GROUPS[group].children = list(map(lambda x: GROUPS[x], inheritance[group]))

    class AppGroup:
        def __init__(self, name, apps):
            self.name = name
            self.apps = apps

    class App:
        def __init__(self, name, app_instances):
            self.name = name
            self.app_instance = app_instances

    class AppInstance:
        def __init__(self, name, requires=[]):
            self.name = name
            self.requires = list(map(lambda x: GROUPS[x], requires))

    app_struct = json.load(open("pages/auth_pages/app_auth.json", 'r'))
    app_instances = {k['name']: AppInstance(k['name']) for i in app_struct['app_groups'] for j in i['apps'] for k in j['instances']}
    app_requires = {k['name']: k['requires'] for i in app_struct['app_groups'] for j in i['apps'] for k in j['instances']}
    apps = {k['name']: App(k['name'], app_instances=list(map(lambda x: app_instances[x['name']], k['instances']))) for i in app_struct['app_groups'] for k in i['apps']}
    app_groups = {k['name']: AppGroup(k['name'], apps=list(map(lambda x: apps[x['name']], k['apps']))) for k in app_struct['app_groups']}
    for app in app_instances:
        app_instances[app].requires = list(map(lambda x: GROUPS[x], app_requires[app]))

    return GROUPS, app_groups, app_instances, apps


def build_elements(init_user):
    user_groups_passed = list_cognito_groups_for_user(init_user)
    cognito_groups = list_all_user_groups()

    GROUPS, app_groups, app_instances, apps = create_app_auth_structure()

    all_user_groups = list(GROUPS.values())
    all_app_groups = list(app_groups.values())
    all_apps = list(app_instances.values())
    app_names = list(app_instances.keys())

    node_cache = {}
    def propagate_inheritance(groups, inherited=False):
        res = set()
        for group in groups:
            group.access = True
            group.inherited = inherited
            if group.children:
                res |= propagate_inheritance(group.children, True)
            res.add(group.name)
        return res

    def gather_requirements(app):
        return set([i.name for i in app.requires])

    def has_app_access(groups, app):
        groups = {k: GROUPS[k] for k in groups}.values()
        inherited_groups = propagate_inheritance(groups)
        requirements = gather_requirements(app)
        return has_access(inherited_groups, requirements)

    def build_app_nodes(user_groups, app_groups):
        nodes = [{'data': {'id': 'apps'}}]
        for app_group in app_groups:
            outer_access = False
            outer = {
                'data': {'id': app_group.name, 'parent': 'apps'},
            }
            for app in app_group.apps:
                inner_access = False
                inner = {
                    'data': {'id': app.name, 'parent': app_group.name}
                }
                for app_instance in app.app_instance:
                    has_access = has_app_access(user_groups, app_instance)
                    inner_access |= has_access
                    outer_access |= has_access
                    color = 'green' if has_access else 'grey'
                    nodes.append({
                        'data': {'id': app_instance.name, 'parent': app.name},
                        'style': {
                            'background-color': color, 'color': 'blue' if has_access else 'grey',
                            'shape': 'triangle',
                        }
                    })
                inner['style'] = {
                    'color': 'blue' if inner_access else 'grey'
                }
                nodes.append(inner)

            outer['style'] = {
                'color': 'blue' if outer_access else 'grey'
            }
            nodes.append(outer)
        return nodes

    def build_group_nodes(groups):
        res = []
        for group in groups:
            if group.inherited:
                background_color = 'green'
            elif group.access:
                background_color = 'lime'
            else:
                background_color = 'grey'
            color = 'black' if group.name in cognito_groups else 'red'
            desc = cognito_groups[group.name] if group.name in cognito_groups else "Warning! Not in Cognito"
            res += [{
                'data': {'id': group.name, 'desc': desc},
                'style': {
                    'color': color,
                    'background-color': background_color,
                    'text-valign': 'bottom'
                }}]
            if group.children:
                res += build_group_nodes(group.children)
        return res

    def build_group_edges(groups):
        res = []
        for group in groups:
            if group.children:
                for child in group.children:
                    res.append({
                        'data': {
                            'id': f'{child.name}-{group.name}',
                            'source': group.name,
                            'target': child.name
                        },
                        'style': {
                            'line-color': 'green' if group.access else 'grey'
                        }
                    })
                    res += build_group_edges(group.children)
        return res

    def build_app_req_edges(apps):
        res = []
        for app in apps:
            if app.requires:
                for req in app.requires:
                    res.append({
                        'data': {
                            'id': f'{app.name}-{req.name}',
                            'source': app.name,
                            'target': req.name
                        },
                        'style': {
                            'line-color': 'green' if req.access else 'grey'
                        }
                    })
        return res


    directed_elements = build_app_nodes(user_groups_passed, all_app_groups) + build_group_nodes(all_user_groups) + build_group_edges(all_user_groups) + build_app_req_edges(all_apps)
    for item in directed_elements:
        m = item['data']
        if m['id'] not in node_cache:
            val = m['id'] + uuid()
            node_cache[m['id']] = val
        val = node_cache[m['id']]
        prev_name = m['id']
        m['id'] = val
        m['name'] = prev_name
    for item in directed_elements:
        m = item['data']
        if 'parent' in m:
            if m['parent'] not in node_cache:
                val = m['parent'] + uuid()
                node_cache[m['parent']] = val
            m['parent'] = node_cache[m['parent']]
    for item in directed_elements:
        m = item['data']
        if 'source' in m:
            if m['source'] not in node_cache:
                val = m['source'] + uuid()
                node_cache[m['source']] = val
            m['source'] = node_cache[m['source']]
    for item in directed_elements:
        m = item['data']
        if 'target' in m:
            if m['target'] not in node_cache:
                val = m['target'] + uuid()
                node_cache[m['target']] = val
            m['target'] = node_cache[m['target']]

    root_string = root_id_string(map(lambda x: node_cache[x], app_names))
    root_layout={
        'name': 'breadthfirst',
        'roots': root_string,
    }
    return directed_elements, root_layout

def layout():
    init_user = 'davinci'
    elements, roots = build_elements(init_user)
    res = html.Div([
        dmc.Title('Authorization Controller', italic=True, order=1),
        dmc.Divider(label='Add/remove user-groups and visualize group inheritance + required app groups.'),
        cyto.Cytoscape(
            id='cyto-graph',
            # layout={'name': 'breadthfirst'},
            layout=roots,
            responsive=True,
            style={'width': '100%', 'height': '80vh'},
            elements=elements,
            stylesheet=[
                {
                    'selector': 'node',
                    'style': {
                        'label': 'data(name)',
                        # 'color': 'black',
                    }
                },
                {
                    'selector': 'node:selected',
                    'style': {
                        'label': 'data(name)',
                        'color': 'blue'
                    }
                },
                {
                    'selector': 'edge',
                    'style': {
                        # The default curve style does not work with certain arrows
                        'curve-style': 'taxi',
                        'target-arrow-color': 'black',
                        'target-arrow-shape': 'triangle-tee',
                    }
                },
            ]
        ),
        dmc.Group([
            dmc.Select(
                id="user-group-selector",
                value=init_user,
                searchable=True,
                data=[
                    {'value': k, 'label': k} for k in user_df['Username'] 
                ],
            ),
            dmc.Button("Add User Groups", id='add_user_group', variant="light"),
            dmc.Button("Delete User Groups", id='delete_user_group', variant="outline"),
            dmc.Text(id="user-group-selected", children=f"Current User: {init_user}"),
            dmc.Text(id="cyto-selected-group"),
            dmc.Text(id="cyto-group-info"),
            html.Div(id='group-change-notification'),
        ], position='apart', grow=True)
    ])
    return res

@callback(Output('cyto-group-info', 'children'),
              Input('cyto-graph', 'mouseoverNodeData'))
def displayTapNodeData(data):
    if data and 'desc' in data:
        return "Group Description: " + data['desc']

@app.callback(Output("user-group-selected", "children"), Input("user-group-selector", "value"))
def select_value(value):
    return 'Current User: ' + value

@app.callback(
    Output("cyto-graph", "elements", allow_duplicate=True),
    Output("cyto-graph", "layout", allow_duplicate=True),
    Input("user-group-selector", 'value'),
    prevent_initial_call=True
)
def update_graph(user):
    elements, roots = build_elements(user)
    return elements, roots

@app.callback(
    Output('cyto-selected-group', 'children'),
    Input('cyto-graph', 'selectedNodeData')
)
def displaySelectedNodeData(data_list):
    if not data_list:
        return ""

    cities_list = [data['name'] for data in data_list]
    return "User Groups Selected To Change: " + ", ".join(cities_list)

def notification_handler(group_list, text):
    if group_list:
        title = 'Success!'
        confirmation = f"Groups {text}: " + ", ".join(group_list)
        color = "green"
        icon = 'mdi:check-bold'
    else:
        title = 'Warning!'
        confirmation = f"No groups {text}!"
        color = "red"
        icon = "svg-spinners:pulse-rings-multiple"
    return dmc.Notification(
        title=title,
        id="simple-notify",
        action="show",
        color=color,
        message=confirmation,
        autoClose=10000,
        icon=DashIconify(icon=icon),
    )

@app.callback(
    Output("cyto-graph", "elements", allow_duplicate=True),
    Output("cyto-graph", "layout", allow_duplicate=True),
    Output("group-change-notification", "children", allow_duplicate=True),
    Input("add_user_group", 'n_clicks'),
    State('user-group-selector', 'value'),
    State('cyto-graph', 'selectedNodeData'),
    prevent_initial_call=True
)
def add_user_to_group(val, user, groups):
    groups = set([k['name'] for k in groups])
    user_groups = set(list_cognito_groups_for_user(user))
    needed_groups = groups - user_groups
    for group in needed_groups:
        add_cognito_group_user(user, group)
    time.sleep(0.25)
    elements, roots = build_elements(user)
    new_user_groups = set(list_cognito_groups_for_user(user))
    diff = list(new_user_groups - user_groups)
    if SYSTEM:
        try:
            r = get_redis_db()
            inject_user_group_redis(user, new_user_groups, r)
        except Exception as e:
            print(e)
    return elements, roots, notification_handler(diff, 'added')

@app.callback(
    Output("cyto-graph", "elements", allow_duplicate=True),
    Output("cyto-graph", "layout", allow_duplicate=True),
    Output("group-change-notification", "children", allow_duplicate=True),
    Input("delete_user_group", 'n_clicks'),
    State('user-group-selector', 'value'),
    State('cyto-graph', 'selectedNodeData'),
    prevent_initial_call=True
)
def remove_user_from_group(val, user, groups):
    groups = set([k['name'] for k in groups])
    user_groups = set(list_cognito_groups_for_user(user))
    needed_groups = groups & user_groups
    for group in needed_groups:
        remove_cognito_group_user(user, group)
    time.sleep(0.25)
    elements, roots = build_elements(user)
    new_user_groups = set(list_cognito_groups_for_user(user))
    diff = list(user_groups - new_user_groups)
    if SYSTEM:
        try:
            r = get_redis_db()
            deject_user_group_redis(user, diff, r)
        except Exception as e:
            print(e)
    return elements, roots, notification_handler(diff, 'removed')

if __name__ == '__main__':
    app.run_server(
        debug=True
    )