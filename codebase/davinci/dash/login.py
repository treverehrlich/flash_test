from flask_login import LoginManager, UserMixin
from davinci.services.auth import get_secret
from davinci.services.s3 import get_file
from davinci.utils.global_config import SYSTEM
import hashlib
import json


def has_access(groups: set, requirements: set) -> bool:
    """
    Logic to ensure that requirements is a subset
    or the user groups. Exception includes when the
    user has the 'dev' or 'superuser' group. If
    the 'superuser' group is a requirement, 'dev'
    will not have access.

    :param groups: a set of groups that the user has.
    :param requirements: a set of groups to check against.
    """
    elevated_priveleges = {'superuser'}
    if 'superuser' not in requirements:
        elevated_priveleges.add('dev')
    return (requirements <= groups) or bool((elevated_priveleges & groups))

def access_check(user, app):
    """
    Helper to check is a user has access to a set of groups.
    If the user shares one group of 'groups', return True.

    :param user: the current_user from Flask
    :param groups: a set of groups to check against.
    """
    dev = hasattr(user, 'username') and user.username == 'Dev'
    if (hasattr(user, 'is_authenticated') and user.is_authenticated) or dev:
        if hasattr(user, 'app_access') or dev:
            return (app in user.app_access) or dev
    return False


def read_only_access(user):
    """
    Helper to check is a user is a read-only user.
    Returns a boolean True if so.

    :param user: the current_user from Flask
    """
    if (hasattr(user, 'is_authenticated') and user.is_authenticated):
        if hasattr(user, 'groups') and hasattr(user, 'inherited_groups'):
            return 'read_only' in user.groups | user.inherited_groups
    return False


def _specific_access_only(group, user):
    """
    Boolean check to see if the user belongs to
    a specific group. Dev and superuser groups also inherit

    :param group: string indicating the group to check against
    :param user: the current_user from Flask

    :return: Boolean
    """
    dev = (hasattr(user, 'username') and user.username == 'Dev') or user.is_anonymous
    if dev: return True
    if (hasattr(user, 'is_authenticated') and user.is_authenticated):
        if hasattr(user, 'groups') and hasattr(user, 'inherited_groups'):
            return any((
                group in user.groups | user.inherited_groups,
                'superuser' in user.groups | user.inherited_groups,
                'dev' in user.groups | user.inherited_groups,
            ))
    return False

def internal_access_only(user):
    """
    Helper to check is a user is an internal user.
    Returns a boolean True if so.

    :param user: the current_user from Flask
    """
    return _specific_access_only('internal', user)


def external_access_only(user):
    """
    Helper to check is a user is an external user.
    Returns a boolean True if so.

    :param user: the current_user from Flask
    """
    return _specific_access_only('external', user)
    

def get_redis_db(): 
    """Helper function to return Redis DB reference."""
    import redis
    redis_db = redis.RedisCluster(
        host=get_secret('AWS_REDIS_DASH_SESSION_DB_URL', doppler=True),
        port=int(get_secret('AWS_REDIS_DASH_SESSION_DB_PORT', doppler=True))
    ) 
    return redis_db


def hash_ip(ip_addr: str) -> str:
    """
    Helper function to hash IP address
    
    :param ip_addr: String IP address
    :return: hashed string
    """
    return hashlib.md5(ip_addr.encode('utf-8')).hexdigest()


def load_auth_structure_redis(redis_db) -> None:
    """
    Load in pre-existing authentication backup JSON files.
    
    :param redis_db: RedisCluster reference
    """

    def _fetch_backup(backup_file):
        get_file(f"dev_backup/{backup_file}", backup_file)
        return json.load(open(backup_file, 'r'))

    app_struct = _fetch_backup("dash_app_app_auth_bck.json")
    group_struct = _fetch_backup("dash_app_group_bck.json")
    ad_group_struct = _fetch_backup("dash_app_ad_group_bck.json")
    redis_db.json().set('apps', '$', app_struct)
    redis_db.json().set('user_groups', '$', group_struct)
    redis_db.json().set('ad_groups', '$', ad_group_struct)
    redis_db.set('auth_struct_loaded', 1)

    
def ensure_auth_struct_loaded(redis_db) -> None:
    """
    Check boolean cached value (in redis) for auth_struct state.
    This value is 0 (false) if the auth_struct needs to be reloaded from backup.
    
    :param redis_db: RedisCluster reference
    """
    try:
        auth_struct_loaded = int(str(redis_db.get('auth_struct_loaded').decode('UTF-8')))
    except:
        auth_struct_loaded = 0
    if not auth_struct_loaded:
        load_auth_structure_redis(redis_db)

def propagate_inheritance(groups: set, redis_db, seen=None) -> set:
    """
    Use the 'user_groups' redis JSON object to determine
    all inherited groups based on the passed 'groups'.
    
    :param groups: set of user groups
    :param redis_db: RedisCluster reference
    :param seen: set of already seen groups to prevent infinite recursion
    """
    ensure_auth_struct_loaded(redis_db)
    user_struct = redis_db.json().get('user_groups')
    ad_struct = redis_db.json().get('ad_groups')
    inherited = set()
    seen = seen if seen else set(groups)
    for group in groups:
        children = user_struct.get(group, [])
        children = set(filter(lambda x: x not in seen, children))
        ad_children = ad_struct.get(group, [])
        children |= set(filter(lambda x: x not in seen, ad_children))
        inherited |= children
        seen |= children
        if children:
            inherited |= propagate_inheritance(children, redis_db, seen)
    for group in groups | inherited:
        if 'QSR' in group:
            inherited.add('QSR')
            break
    return inherited


def propagate_app_access(groups, redis_db):
    """
    Use the 'apps' redis JSON object to determine
    all apps the groups grant access to.
    
    :param groups: set of user groups
    :param redis_db: RedisCluster reference
    """
    ensure_auth_struct_loaded(redis_db)

    app_access = set()
    render_app = set()
    render_section = set()
    apps = redis_db.json().get('apps', '$.[*].[*]')
    app_struct = redis_db.json().get('apps')
    app_group_mapping = {}
    app_mapping = {}    
    for app_group in app_struct:
        for app in app_struct[app_group]:
            for app_inst in app_struct[app_group][app]:
                app_group_mapping[app_inst] = app_group
                app_mapping[app_inst] = app
            
    for app in apps:
        for app_inst in app:
            requirements = app[app_inst]
            if has_access(groups, set(requirements)):
                app_name = app_inst
                app_access.add(app_name)
                app_ = app_mapping[app_name]
                app_group = app_group_mapping[app_name]
                render_app.add(app_)
                render_section.add(app_group)
    return app_access, render_app, render_section


# Create User class with UserMixin
class User(UserMixin):
    """User class for the underlying flask app. This is what defines
    what properties will exist for our user."""
    def __init__(self, username: str, uid: str, first_name: str=None, groups: set=set()):
        """
        :param username: username
        :param uid: unique identifier
        :param first_name: user first name
        :param groups: set of groups the user belongs to
        """
        self.id = uid
        self.username = username
        self.first_name = "Missing Name!" if not first_name else first_name
        self.groups = set(groups)
        
        if self.username.lower().endswith('@kencogroup.com'):
            self.groups.add('internal')
        else:
            self.groups.add('external')

        if SYSTEM:
            r = get_redis_db()
            self.inherited_groups = propagate_inheritance(self.groups, r)
            self.app_access, self.apps, self.accordions = propagate_app_access(self.groups | self.inherited_groups, r)
        else:
            self.inherited_groups = set()
            self.app_access, self.apps, self.accordions = set(), set(), set()


    def get_id(self):
        """Fetch the uid set on instantiation."""
        return self.id


DEV_USER = User('Dev', '111', first_name='dev', groups={'superuser'})
DEV_USER.accordions = {'admin', 'standard_apps', 'custom_apps', 'data_portals'}
DEV_USER.app_access = {
    'slot_dc',
    'order_mgmt',
    'kencogpt',
    'chervon_sop_genie',
    'bluebuff_pick_path',
    'avrl_pricing',
    'auto_ml_forecast',
    'portal_kls',
    'portal_lms',
    'auth_controller',
}
"""A mock user to work with on Windows development stations."""


def write_user_to_redis(user: User, redis_db):
    """
    Helper function to write User info to Redis DB
    
    :param user: User object with info.
    :param redis_db: RedisCluster object
    """
    redis_db.set(user.id, 1)
    redis_db.set(user.username, user.id)
    redis_db.set(user.id + '_username', user.username)
    redis_db.set(user.id + '_first_name', user.first_name)
    for group in user.groups:
        redis_db.sadd(user.id + '_groups', group)
    refresh_user_redis(user, redis_db)


def refresh_user_redis(user: User, redis_db, ttl=1800):
    """
    Helper function to refresh the timeout on User info in Redis DB
    
    :param user: User object with info.
    :param redis_db: RedisCluster object
    :param ttl: TimeToLive 
    """
    redis_db.expire(user.id, ttl)
    redis_db.expire(user.username, ttl)
    redis_db.expire(user.id + '_username', ttl)
    redis_db.expire(user.id + '_first_name', ttl)
    redis_db.expire(user.id + '_groups', ttl)


def delete_user_redis(user: User, redis_db):
    """
    Helper function to delete user info from Redis DB
    
    :param user: User object with info.
    :param redis_db: RedisCluster object
    """
    redis_db.delete(user.id)
    redis_db.delete(user.username)
    redis_db.delete(user.id + '_username')
    redis_db.delete(user.id + '_first_name')
    redis_db.delete(user.id + '_groups')


def inject_user_group_redis(username, groups, redis_db):
    """
    Adds groups to the user on-the-fly (no re-login required)
    
    :param username: Username to attempt fetching
    :param groups: list of groups to add to the user session
    :param redis_db: RedisCluster object
    """
    session_id = redis_db.get(username)
    if session_id:
        for group in groups:
            redis_db.sadd(session_id.decode('UTF-8') + '_groups', group)


def deject_user_group_redis(username, groups, redis_db):
    """
    Removes groups from the user on-the-fly (no re-login required)
    
    :param username: Username to attempt fetching
    :param groups: list of groups to add to the user session
    :param redis_db: RedisCluster object
    """
    session_id = redis_db.get(username)
    if session_id:
        for group in groups:
            redis_db.srem(session_id.decode('UTF-8') + '_groups', group)


def fetch_user_redis(user_id: str, redis_db):
    """
    Helper function to query User info on Redis DB
    
    :param user_id: the unique identifier for the User
    :param redis_db: RedisCluster object
    """
    try:
        has_key = redis_db.get(user_id)
        if has_key:
            username = redis_db.get(user_id + '_username').decode('UTF-8')
            first_name = redis_db.get(user_id + '_first_name').decode('UTF-8')
            groups = set(map(lambda x: x.decode('UTF-8'), redis_db.smembers(user_id + '_groups')))
            user = User(username, user_id, first_name, groups=groups)
            return user
        return None
    except Exception as e:
        print(e)
        return None


def login_manager_user_loader_factory(server):
    """
    Function factory that creates the user loader
    required by Flask.
    
    :param server: flask Server instance
    :return: the same flask Server, load_user function, and flask login_manager
    :rtype: server, function, decorator
    """
    # Setup the LoginManager for the server
    login_manager = LoginManager()
    login_manager.init_app(server)
    login_manager.login_view = "/login"

    server.config.update(
        SECRET_KEY=get_secret("DASH_SERVER_SECRET_KEY", doppler=True),
    )

    if not SYSTEM:
        # Mock loader that won't hit Redis, useful for local dev
        @login_manager.user_loader
        def load_user(user_id):
            return DEV_USER
            
    else:
        # Actual user loader
        redis_db = get_redis_db()
        @login_manager.user_loader
        def load_user(user_id):
            print(f"attempted {user_id}")
            user = fetch_user_redis(user_id, redis_db)
            if user:
                print(user.first_name, user.groups, user.inherited_groups)
                refresh_user_redis(user, redis_db)
                return user
            else:
                return None
    return load_user, login_manager
