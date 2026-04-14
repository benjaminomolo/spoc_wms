from flask import Blueprint, request, jsonify

from db import Session
from models import ContactMessage

public_routes = Blueprint('public_routes', __name__)


@public_routes.route('/contact', methods=['POST'])
def handle_contact():
    data = request.get_json()

    name = data.get('name', '').strip()
    email = data.get('email', '').strip()
    message = data.get('message', '').strip()

    # Simple validation
    if not name or not email or not message:
        return jsonify({'success': False, 'message': 'All fields are required.'}), 400

    with Session() as session:
        try:
            contact_message = ContactMessage(
                name=name,
                email=email,
                message=message
            )
            session.add(contact_message)
            session.commit()

            return jsonify({'success': True})
        except Exception as e:
            session.rollback()
            print(f"Error saving contact message: {e}")
            return jsonify({'success': False, 'message': 'Failed to save your message.'}), 500
