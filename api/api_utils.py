from db_models import Users, db
from werkzeug.security import generate_password_hash
import uuid
from functools import wraps
from flask import jsonify, request
import jwt
from app import app


def token_required(f):
    
    '''
    Wrapper function for routes for which a token authentification 
    is needed.
    
    Parameters
    ----------
    f : function
        The function to be wrapped.
    
    Returns
    -------
    function
        As the function is returned it is excuted.
    '''
    
    @wraps(f)
    def decorator(*args, **kwargs):

        token = None

        if 'x-access-token' in request.headers:
           token = request.headers['x-access-token']

        if not token:
           return jsonify({'message': r'''a valid token is missing. Set request headers: headers = {'content-type': 'application/json','x-access-token': <token>}'''})

        try:
           data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
# =============================================================================
#            current_user = Users.query.filter_by(
#            public_id=data['public_id']).first()
# =============================================================================
        except:
            return jsonify({'message': 'token is invalid'})
        
        return f(*args, **kwargs)
    
    return decorator


def delete_all_users():
    """
    Delete all users from the database.
    
    Returns
    -------
    str
        A message indicating the result of the deletion attempt.
    """
    try:
        # Delete all users from the Users table
        num_deleted = db.session.query(Users).delete()
        db.session.commit()
        
        # Check if any users were deleted
        if num_deleted > 0:
            return jsonify({'message': f'Successfully deleted {num_deleted} users.'}), 200
        else:
            return jsonify({'message': 'No users found to delete.'}), 404
    except Exception as e:
        # In case of an error during deletion, log it and return an error message
        # You should configure logging appropriately for your application
        print(f"Error deleting all users: {e}")  # Consider using logging instead of print in production
        return jsonify({'message': 'Internal server error during the deletion of all users.'}), 500


def delete_user(name):
    """
    Delete a user from the database by their name.

    Parameters
    ----------
    name : str
        The name of the user to delete.
    
    Returns
    -------
    str
        A message indicating the result of the deletion attempt.
    """
    # Attempt to find the user by name
    user = Users.query.filter_by(name=name).first()
    
    # If the user does not exist, return an error message
    if user is None:
        return jsonify({'message': f'User "{name}" not found.'}), 404
    
    try:
        # If the user exists, delete them and commit the changes
        db.session.delete(user)
        db.session.commit()
        return jsonify({'message': f'User "{name}" deleted successfully.'}), 200
    except Exception as e:
        # In case of an error during deletion, log it and return an error message
        # You should configure logging appropriately for your application
        print(f"Error deleting user: {e}")  # Consider using logging instead of print in production
        return jsonify({'message': 'Internal server error during user deletion.'}), 500


def signup_user(name, pw):   
    
    '''
    Register a user in the database. 
    
    Parameters
    ----------
    name : string
        The users name
    pw : string
        The users password
    '''
    
    #Check if user exists
    user = Users.query.filter(Users.name == name).all()
    
    if user:
        return f'user "{name}" already exists.'
    
    hashed_password = generate_password_hash(pw)
    
    new_user = Users(public_id=str(uuid.uuid4()), name=name, password=hashed_password, admin=False) 
    db.session.add(new_user)  
    db.session.commit()    
    
    return f'user "{name}" registered successfully'


def get_all_users():  
    
    '''
    Get all registered users.
    
    Returns
    -------
    list
        List with users and attributes.
    
    '''
    
    users = Users.query.all() 
    
    result = []   
    
    for user in users:   
        user_data = {}   
        user_data['public_id'] = user.public_id  
        user_data['name'] = user.name 
        user_data['password'] = user.password
        user_data['admin'] = user.admin 
        
        result.append(user_data)   
    
    return dict({'users': result})