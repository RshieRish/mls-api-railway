#!/usr/bin/env python3
"""
Railway deployment version of the MLS API
Connects to PostgreSQL database for real CRM data
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("mls_api")

# Database configuration
DB_DSN = os.getenv("DATABASE_URL")

def get_conn():
    """Get database connection"""
    return psycopg2.connect(dsn=DB_DSN, cursor_factory=RealDictCursor)

def format_listing_data(raw_data):
    """Format raw database listing data for API response"""
    if not raw_data or 'data' not in raw_data:
        return None
    
    data = raw_data['data']
    
    # Extract data from _raw_data if available, otherwise use direct data
    if '_raw_data' in data:
        source_data = data['_raw_data']
    else:
        source_data = data
    
    # Check if this is Brandon's listing
    agent_id = source_data.get('LIST_AGENT', '')
    is_brandon = agent_id == 'CN222505'
    
    formatted = {
        "LIST_NO": raw_data.get('listing_key', source_data.get('ListingKey', '')),
        "LIST_PRICE": source_data.get('LIST_PRICE', source_data.get('ListPrice', '')),
        "STREET_NO": source_data.get('STREET_NUMBER', ''),
        "STREET_NAME": source_data.get('STREET_NAME', source_data.get('StreetName', '')),
        "City": source_data.get('CITY', source_data.get('City', '')),
        "STATE": source_data.get('STATE', source_data.get('StateOrProvince', '')),
        "ZIP_CODE": source_data.get('ZIP_CODE', source_data.get('PostalCode', '')),
        "NO_BEDROOMS": source_data.get('NO_BEDROOMS', source_data.get('BedroomsTotal', '')),
        "NO_FULL_BATHS": source_data.get('NO_FULL_BATHS', source_data.get('BathroomsFull', '')),
        "SQUARE_FEET": source_data.get('SQUARE_FEET', source_data.get('LivingArea', '')),
        "STATUS": source_data.get('STATUS', source_data.get('ListingStatus', '')),
        "REMARKS": source_data.get('REMARKS', source_data.get('PublicRemarks', '')),
        "LATITUDE": source_data.get('LATITUDE', source_data.get('Latitude', '')),
        "LONGITUDE": source_data.get('LONGITUDE', source_data.get('Longitude', '')),
        "LIST_AGENT_NAME": "Brandon Sweeney" if is_brandon else source_data.get('LIST_AGENT_NAME', ''),
        "LIST_AGENT_ID": agent_id,
        "IS_BRANDON_LISTING": is_brandon
    }
    
    return formatted

# FastAPI app
app = FastAPI(
    title="MLS API for Railway",
    version="1.0.0",
    description="MLS listings API deployed on Railway",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class Listing(BaseModel):
    data: Dict[str, Any]

# CRM Pydantic Models
class Contact(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    primary_personal_email: Optional[str] = None
    primary_personal_phone: Optional[str] = None
    mailing_address_line1: Optional[str] = None
    mailing_address_line2: Optional[str] = None
    mailing_address_city: Optional[str] = None
    mailing_address_state: Optional[str] = None
    mailing_address_zip: Optional[str] = None
    lead_source: Optional[str] = None
    notes: Optional[str] = None

class ContactUpdate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    primary_personal_email: Optional[str] = None
    primary_personal_phone: Optional[str] = None
    mailing_address_line1: Optional[str] = None
    mailing_address_line2: Optional[str] = None
    mailing_address_city: Optional[str] = None
    mailing_address_state: Optional[str] = None
    mailing_address_zip: Optional[str] = None
    lead_source: Optional[str] = None
    notes: Optional[str] = None

class Neighborhood(BaseModel):
    name: str
    geojson: Optional[str] = None
    share_link: Optional[str] = None

class NeighborhoodUpdate(BaseModel):
    name: Optional[str] = None
    geojson: Optional[str] = None
    share_link: Optional[str] = None

class TimelineEvent(BaseModel):
    contact_id: int
    occurred_at: str
    type: str
    title: str
    body: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = {}
    actor: Optional[str] = None

class Task(BaseModel):
    contact_id: int
    title: str
    description: Optional[str] = None
    due_date: Optional[str] = None
    priority: str = "medium"
    status: str = "pending"

@app.get("/")
def root():
    return {
        "message": "MLS API is running on Railway",
        "version": "1.0.0",
        "endpoints": [
            "/listings",
            "/listings/{listing_id}",
            "/listings/featured",
            "/search",
            "/health",
            "/api/crm/contacts",
            "/api/crm/contacts/{contact_id}",
            "/api/crm/neighborhoods",
            "/api/crm/neighborhoods/{neighborhood_id}",
            "/api/crm/contacts/{contact_id}/neighborhoods",
            "/api/crm/contacts/{contact_id}/timeline",
            "/api/crm/contacts/{contact_id}/tasks",
            "/api/crm/contacts/{contact_id}/notes",
            "/api/crm/tasks/{task_id}",
            "/api/crm/notes/{note_id}"
        ]
    }

# CRM API Endpoints

# Contacts endpoints
@app.get("/api/crm/contacts")
def get_contacts(
    search: Optional[str] = Query(None),
    segment: str = Query("All"),
    sort: str = Query("updated_at"),
    order: str = Query("DESC"),
    limit: int = Query(25),
    offset: int = Query(0)
):
    """Get contacts with search, filtering, and pagination"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            # Build search conditions
            where_conditions = []
            params = []
            
            if search:
                where_conditions.append(
                    "(full_name ILIKE %s OR primary_personal_email ILIKE %s OR primary_personal_phone ILIKE %s)"
                )
                search_term = f"%{search}%"
                params.extend([search_term, search_term, search_term])
            
            # Build query
            base_query = """
                SELECT id, first_name, last_name, full_name, primary_personal_email, 
                       primary_personal_phone, mailing_city, mailing_state,
                       health_score, last_contacted_at, created_at, updated_at
                FROM crm_contacts
            """
            
            if where_conditions:
                query = f"{base_query} WHERE {' AND '.join(where_conditions)}"
            else:
                query = base_query
            
            # Add ordering and pagination
            query += f" ORDER BY {sort} {order} LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            
            cur.execute(query, params)
            contacts = cur.fetchall()
            
            # Get total count
            count_query = "SELECT COUNT(*) FROM crm_contacts"
            if where_conditions:
                count_query += f" WHERE {' AND '.join(where_conditions)}"
                cur.execute(count_query, params[:-2])  # Exclude limit/offset
            else:
                cur.execute(count_query)
            
            total = cur.fetchone()[0]
            
            return {
                "contacts": [dict(contact) for contact in contacts],
                "total": total,
                "limit": limit,
                "offset": offset
            }
            
    except Exception as e:
        log.error(f"Error fetching contacts: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.get("/api/crm/contacts/{contact_id}")
def get_contact(contact_id: int):
    """Get a specific contact by ID"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM crm_contacts WHERE id = %s
            """, (contact_id,))
            
            contact = cur.fetchone()
            if not contact:
                raise HTTPException(status_code=404, detail="Contact not found")
            
            return dict(contact)
            
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error fetching contact {contact_id}: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.post("/api/crm/contacts")
def create_contact(contact: Contact):
    """Create a new contact"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            # Calculate full_name
            full_name = f"{contact.first_name or ''} {contact.last_name or ''}".strip()
            
            cur.execute("""
                INSERT INTO crm_contacts (
                    first_name, last_name, full_name, primary_personal_email,
                    primary_personal_phone, mailing_address_line1, mailing_address_line2,
                    mailing_address_city, mailing_address_state, mailing_address_zip,
                    lead_source, notes, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                RETURNING *
            """, (
                contact.first_name, contact.last_name, full_name,
                contact.primary_personal_email, contact.primary_personal_phone,
                contact.mailing_address_line1, contact.mailing_address_line2,
                contact.mailing_address_city, contact.mailing_address_state,
                contact.mailing_address_zip, contact.lead_source, contact.notes
            ))
            
            new_contact = cur.fetchone()
            
            # Update health score
            cur.execute("UPDATE crm_contacts SET health_score = calculate_health_score(id) WHERE id = %s", (new_contact['id'],))
            
            conn.commit()
            return dict(new_contact)
            
    except Exception as e:
        log.error(f"Error creating contact: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.put("/api/crm/contacts/{contact_id}")
def update_contact(contact_id: int, contact: ContactUpdate):
    """Update a contact"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            # Build update fields
            update_fields = []
            params = []
            
            for field, value in contact.dict(exclude_unset=True).items():
                update_fields.append(f"{field} = %s")
                params.append(value)
            
            if not update_fields:
                raise HTTPException(status_code=400, detail="No fields to update")
            
            # Add full_name update if first_name or last_name changed
            if contact.first_name is not None or contact.last_name is not None:
                # Get current values
                cur.execute("SELECT first_name, last_name FROM crm_contacts WHERE id = %s", (contact_id,))
                current = cur.fetchone()
                if current:
                    new_first = contact.first_name if contact.first_name is not None else current['first_name']
                    new_last = contact.last_name if contact.last_name is not None else current['last_name']
                    full_name = f"{new_first or ''} {new_last or ''}".strip()
                    update_fields.append("full_name = %s")
                    params.append(full_name)
            
            update_fields.append("updated_at = NOW()")
            params.append(contact_id)
            
            query = f"UPDATE crm_contacts SET {', '.join(update_fields)} WHERE id = %s RETURNING *"
            cur.execute(query, params)
            
            updated_contact = cur.fetchone()
            if not updated_contact:
                raise HTTPException(status_code=404, detail="Contact not found")
            
            # Update health score
            cur.execute("UPDATE crm_contacts SET health_score = calculate_health_score(id) WHERE id = %s", (contact_id,))
            
            conn.commit()
            return dict(updated_contact)
            
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error updating contact {contact_id}: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.delete("/api/crm/contacts/{contact_id}")
def delete_contact(contact_id: int):
    """Delete a contact"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM crm_contacts WHERE id = %s RETURNING id", (contact_id,))
            deleted = cur.fetchone()
            
            if not deleted:
                raise HTTPException(status_code=404, detail="Contact not found")
            
            conn.commit()
            return {"message": "Contact deleted successfully"}
            
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error deleting contact {contact_id}: {e}")
        raise HTTPException(status_code=500, detail="Database error")

# Neighborhoods endpoints
@app.get("/api/crm/neighborhoods")
def get_neighborhoods():
    """Get all neighborhoods"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, geojson, share_link, created_at
                FROM crm_neighborhoods
                ORDER BY name
            """)
            
            neighborhoods = cur.fetchall()
            return [dict(neighborhood) for neighborhood in neighborhoods]
            
    except Exception as e:
        log.error(f"Error fetching neighborhoods: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.get("/api/crm/neighborhoods/{neighborhood_id}")
def get_neighborhood(neighborhood_id: int):
    """Get a specific neighborhood by ID"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM crm_neighborhoods WHERE id = %s
            """, (neighborhood_id,))
            
            neighborhood = cur.fetchone()
            if not neighborhood:
                raise HTTPException(status_code=404, detail="Neighborhood not found")
            
            return dict(neighborhood)
            
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error fetching neighborhood {neighborhood_id}: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.post("/api/crm/neighborhoods")
def create_neighborhood(neighborhood: Neighborhood):
    """Create a new neighborhood"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO crm_neighborhoods (name, geojson, share_link, created_at)
                VALUES (%s, %s, %s, NOW())
                RETURNING *
            """, (neighborhood.name, neighborhood.geojson, neighborhood.share_link))
            
            new_neighborhood = cur.fetchone()
            conn.commit()
            return dict(new_neighborhood)
            
    except Exception as e:
        log.error(f"Error creating neighborhood: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.put("/api/crm/neighborhoods/{neighborhood_id}")
def update_neighborhood(neighborhood_id: int, neighborhood: NeighborhoodUpdate):
    """Update a neighborhood"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            # Build update fields
            update_fields = []
            params = []
            
            for field, value in neighborhood.dict(exclude_unset=True).items():
                update_fields.append(f"{field} = %s")
                params.append(value)
            
            if not update_fields:
                raise HTTPException(status_code=400, detail="No fields to update")
            
            params.append(neighborhood_id)
            
            query = f"UPDATE crm_neighborhoods SET {', '.join(update_fields)} WHERE id = %s RETURNING *"
            cur.execute(query, params)
            
            updated_neighborhood = cur.fetchone()
            if not updated_neighborhood:
                raise HTTPException(status_code=404, detail="Neighborhood not found")
            
            conn.commit()
            return dict(updated_neighborhood)
            
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error updating neighborhood {neighborhood_id}: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.delete("/api/crm/neighborhoods/{neighborhood_id}")
def delete_neighborhood(neighborhood_id: int):
    """Delete a neighborhood"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM crm_neighborhoods WHERE id = %s RETURNING id", (neighborhood_id,))
            deleted = cur.fetchone()
            
            if not deleted:
                raise HTTPException(status_code=404, detail="Neighborhood not found")
            
            conn.commit()
            return {"message": "Neighborhood deleted successfully"}
            
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error deleting neighborhood {neighborhood_id}: {e}")
        raise HTTPException(status_code=500, detail="Database error")

# Contact-Neighborhood relationship endpoints
@app.get("/api/crm/contacts/{contact_id}/neighborhoods")
def get_contact_neighborhoods(contact_id: int):
    """Get neighborhoods for a specific contact"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT n.id, n.name, n.geojson, n.share_link, cn.is_primary
                FROM crm_neighborhoods n
                JOIN crm_contact_neighborhoods cn ON n.id = cn.neighborhood_id
                WHERE cn.contact_id = %s
                ORDER BY cn.is_primary DESC, n.name
            """, (contact_id,))
            
            neighborhoods = cur.fetchall()
            return [dict(neighborhood) for neighborhood in neighborhoods]
            
    except Exception as e:
        log.error(f"Error fetching contact neighborhoods: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.post("/api/crm/contacts/{contact_id}/neighborhoods")
def add_contact_to_neighborhood(contact_id: int, data: dict):
    """Add a contact to a neighborhood"""
    try:
        neighborhood_id = data.get('neighborhood_id')
        is_primary = data.get('is_primary', False)
        
        if not neighborhood_id:
            raise HTTPException(status_code=400, detail="neighborhood_id is required")
        
        with get_conn() as conn, conn.cursor() as cur:
            # If setting as primary, remove primary flag from other neighborhoods
            if is_primary:
                cur.execute(
                    "UPDATE crm_contact_neighborhoods SET is_primary = FALSE WHERE contact_id = %s",
                    (contact_id,)
                )
            
            # Add the relationship
            cur.execute("""
                INSERT INTO crm_contact_neighborhoods (contact_id, neighborhood_id, is_primary, created_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (contact_id, neighborhood_id) 
                DO UPDATE SET is_primary = %s
                RETURNING *
            """, (contact_id, neighborhood_id, is_primary, is_primary))
            
            relationship = cur.fetchone()
            conn.commit()
            return dict(relationship)
            
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error adding contact to neighborhood: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.delete("/api/crm/contacts/{contact_id}/neighborhoods/{neighborhood_id}")
def remove_contact_from_neighborhood(contact_id: int, neighborhood_id: int):
    """Remove a contact from a neighborhood"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                DELETE FROM crm_contact_neighborhoods 
                WHERE contact_id = %s AND neighborhood_id = %s
                RETURNING *
            """, (contact_id, neighborhood_id))
            
            deleted = cur.fetchone()
            if not deleted:
                raise HTTPException(status_code=404, detail="Relationship not found")
            
            conn.commit()
            return {"message": "Contact removed from neighborhood successfully"}
            
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error removing contact from neighborhood: {e}")
        raise HTTPException(status_code=500, detail="Database error")

# Timeline Events endpoints
@app.get("/api/crm/contacts/{contact_id}/timeline")
def get_contact_timeline(contact_id: int):
    """Get timeline events for a contact"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM crm_timeline_events 
                WHERE contact_id = %s 
                ORDER BY created_at DESC
            """, (contact_id,))
            
            events = cur.fetchall()
            return [dict(event) for event in events]
            
    except Exception as e:
        log.error(f"Error fetching timeline for contact {contact_id}: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.post("/api/crm/contacts/{contact_id}/timeline")
def create_timeline_event(contact_id: int, event: TimelineEvent):
    """Create a new timeline event for a contact"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO crm_timeline_events 
                (contact_id, event_type, title, description, metadata, created_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                RETURNING *
            """, (contact_id, event.event_type, event.title, event.description, event.metadata))
            
            new_event = cur.fetchone()
            conn.commit()
            return dict(new_event)
            
    except Exception as e:
        log.error(f"Error creating timeline event: {e}")
        raise HTTPException(status_code=500, detail="Database error")

# Tasks endpoints
@app.get("/api/crm/contacts/{contact_id}/tasks")
def get_contact_tasks(contact_id: int, status: str = None):
    """Get tasks for a contact"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            if status:
                cur.execute("""
                    SELECT * FROM crm_tasks 
                    WHERE contact_id = %s AND status = %s
                    ORDER BY due_date ASC, created_at DESC
                """, (contact_id, status))
            else:
                cur.execute("""
                    SELECT * FROM crm_tasks 
                    WHERE contact_id = %s
                    ORDER BY due_date ASC, created_at DESC
                """, (contact_id,))
            
            tasks = cur.fetchall()
            return [dict(task) for task in tasks]
            
    except Exception as e:
        log.error(f"Error fetching tasks for contact {contact_id}: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.post("/api/crm/contacts/{contact_id}/tasks")
def create_task(contact_id: int, task: Task):
    """Create a new task for a contact"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO crm_tasks 
                (contact_id, title, description, due_date, priority, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                RETURNING *
            """, (contact_id, task.title, task.description, task.due_date, task.priority, task.status))
            
            new_task = cur.fetchone()
            conn.commit()
            return dict(new_task)
            
    except Exception as e:
        log.error(f"Error creating task: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.put("/api/crm/tasks/{task_id}")
def update_task(task_id: int, task_update: dict):
    """Update a task"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            # Build update fields
            update_fields = []
            params = []
            
            for field, value in task_update.items():
                if field in ['title', 'description', 'due_date', 'priority', 'status']:
                    update_fields.append(f"{field} = %s")
                    params.append(value)
            
            if not update_fields:
                raise HTTPException(status_code=400, detail="No valid fields to update")
            
            params.append(task_id)
            
            query = f"UPDATE crm_tasks SET {', '.join(update_fields)} WHERE id = %s RETURNING *"
            cur.execute(query, params)
            
            updated_task = cur.fetchone()
            if not updated_task:
                raise HTTPException(status_code=404, detail="Task not found")
            
            conn.commit()
            return dict(updated_task)
            
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error updating task {task_id}: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.delete("/api/crm/tasks/{task_id}")
def delete_task(task_id: int):
    """Delete a task"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM crm_tasks WHERE id = %s RETURNING id", (task_id,))
            deleted = cur.fetchone()
            
            if not deleted:
                raise HTTPException(status_code=404, detail="Task not found")
            
            conn.commit()
            return {"message": "Task deleted successfully"}
            
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error deleting task {task_id}: {e}")
        raise HTTPException(status_code=500, detail="Database error")

# Notes endpoints
@app.get("/api/crm/contacts/{contact_id}/notes")
def get_contact_notes(contact_id: int):
    """Get notes for a contact"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM crm_notes 
                WHERE contact_id = %s 
                ORDER BY created_at DESC
            """, (contact_id,))
            
            notes = cur.fetchall()
            return [dict(note) for note in notes]
            
    except Exception as e:
        log.error(f"Error fetching notes for contact {contact_id}: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.post("/api/crm/contacts/{contact_id}/notes")
def create_note(contact_id: int, note_data: dict):
    """Create a new note for a contact"""
    try:
        content = note_data.get('content')
        if not content:
            raise HTTPException(status_code=400, detail="Content is required")
        
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO crm_notes (contact_id, content, created_at)
                VALUES (%s, %s, NOW())
                RETURNING *
            """, (contact_id, content))
            
            new_note = cur.fetchone()
            conn.commit()
            return dict(new_note)
            
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error creating note: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.put("/api/crm/notes/{note_id}")
def update_note(note_id: int, note_data: dict):
    """Update a note"""
    try:
        content = note_data.get('content')
        if not content:
            raise HTTPException(status_code=400, detail="Content is required")
        
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                UPDATE crm_notes SET content = %s WHERE id = %s RETURNING *
            """, (content, note_id))
            
            updated_note = cur.fetchone()
            if not updated_note:
                raise HTTPException(status_code=404, detail="Note not found")
            
            conn.commit()
            return dict(updated_note)
            
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error updating note {note_id}: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.delete("/api/crm/notes/{note_id}")
def delete_note(note_id: int):
    """Delete a note"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM crm_notes WHERE id = %s RETURNING id", (note_id,))
            deleted = cur.fetchone()
            
            if not deleted:
                raise HTTPException(status_code=404, detail="Note not found")
            
            conn.commit()
            return {"message": "Note deleted successfully"}
            
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error deleting note {note_id}: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.get("/health")
def health_check():
    """Health check endpoint"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM listings")
            count = cur.fetchone()[0]
            return {
                "status": "OK",
                "message": "MLS API is running on Railway",
                "listings_count": count,
                "database_connected": True,
                "timestamp": datetime.now().isoformat()
            }
    except Exception as e:
        log.error(f"Health check database error: {e}")
        return {
            "status": "OK",
            "message": "MLS API is running on Railway",
            "listings_count": 0,
            "database_connected": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/listings")
def get_listings(
    city: Optional[str] = Query(None),
    min_price: Optional[int] = Query(None),
    max_price: Optional[int] = Query(None),
    bedrooms: Optional[int] = Query(None),
    limit: int = Query(50, gt=0, le=100)
):
    """Get listings with optional filters from database"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            # Build dynamic query with filters
            where_conditions = []
            params = []
            
            if city:
                where_conditions.append("(data->'_raw_data'->>'CITY' ILIKE %s OR data->>'City' ILIKE %s)")
                params.extend([f"%{city}%", f"%{city}%"])
            
            if min_price:
                where_conditions.append("(CAST(data->'_raw_data'->>'LIST_PRICE' AS INTEGER) >= %s OR CAST(data->>'ListPrice' AS INTEGER) >= %s)")
                params.extend([min_price, min_price])
            
            if max_price:
                where_conditions.append("(CAST(data->'_raw_data'->>'LIST_PRICE' AS INTEGER) <= %s OR CAST(data->>'ListPrice' AS INTEGER) <= %s)")
                params.extend([max_price, max_price])
            
            if bedrooms:
                where_conditions.append("(CAST(data->'_raw_data'->>'NO_BEDROOMS' AS INTEGER) = %s OR CAST(data->>'BedroomsTotal' AS INTEGER) = %s)")
                params.extend([bedrooms, bedrooms])
            
            # Construct the query
            base_query = "SELECT listing_key, data, updated_at FROM listings"
            if where_conditions:
                query = f"{base_query} WHERE {' AND '.join(where_conditions)} ORDER BY updated_at DESC LIMIT %s"
                params.append(limit)
            else:
                query = f"{base_query} ORDER BY updated_at DESC LIMIT %s"
                params.append(limit)
            
            cur.execute(query, params)
            results = cur.fetchall()
            
            # Format results
            formatted_listings = []
            for row in results:
                formatted = format_listing_data(row)
                if formatted:
                    formatted_listings.append({"data": formatted})
            
            log.info(f"Retrieved {len(formatted_listings)} listings from database")
            return formatted_listings
            
    except Exception as e:
        log.error(f"Database error in get_listings: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.get("/listings/featured")
def get_featured_listings():
    """Get featured listings with Brandon Sweeney's listings prioritized first"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            # Get Brandon's listings first
            cur.execute("""
                SELECT listing_key, data, updated_at 
                FROM listings 
                WHERE data::text LIKE '%CN222505%'
                ORDER BY updated_at DESC 
                LIMIT 2
            """)
            brandon_results = cur.fetchall()
            
            # Get other listings
            cur.execute("""
                SELECT listing_key, data, updated_at 
                FROM listings 
                WHERE data::text NOT LIKE '%CN222505%'
                ORDER BY updated_at DESC 
                LIMIT 1
            """)
            other_results = cur.fetchall()
            
            # Combine results with Brandon's first
            all_results = brandon_results + other_results
            
            # Format results
            featured_listings = []
            for row in all_results:
                formatted = format_listing_data(row)
                if formatted:
                    featured_listings.append({"data": formatted})
            
            log.info(f"Retrieved {len(featured_listings)} featured listings from database")
            return featured_listings[:3]  # Ensure max 3 listings
            
    except Exception as e:
        log.error(f"Database error in get_featured_listings: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.get("/listings/{listing_id}")
def get_listing_by_id(listing_id: str):
    """Get a specific listing by ID from database"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT listing_key, data, updated_at 
                FROM listings 
                WHERE listing_key = %s
            """, (listing_id,))
            
            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="Listing not found")
            
            formatted = format_listing_data(result)
            if not formatted:
                raise HTTPException(status_code=404, detail="Listing data invalid")
            
            log.info(f"Retrieved listing {listing_id} from database")
            return {"data": formatted}
            
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Database error in get_listing_by_id: {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.post("/search")
def advanced_search(criteria: dict):
    """Advanced search endpoint with database integration"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            # Build dynamic search query based on criteria
            where_conditions = []
            params = []
            
            # Add search conditions based on criteria
            if criteria.get('city'):
                where_conditions.append("(data->'_raw_data'->>'CITY' ILIKE %s OR data->>'City' ILIKE %s)")
                params.extend([f"%{criteria['city']}%", f"%{criteria['city']}%"])
            
            if criteria.get('min_price'):
                where_conditions.append("(CAST(data->'_raw_data'->>'LIST_PRICE' AS INTEGER) >= %s OR CAST(data->>'ListPrice' AS INTEGER) >= %s)")
                params.extend([criteria['min_price'], criteria['min_price']])
            
            if criteria.get('max_price'):
                where_conditions.append("(CAST(data->'_raw_data'->>'LIST_PRICE' AS INTEGER) <= %s OR CAST(data->>'ListPrice' AS INTEGER) <= %s)")
                params.extend([criteria['max_price'], criteria['max_price']])
            
            if criteria.get('bedrooms'):
                where_conditions.append("(CAST(data->'_raw_data'->>'NO_BEDROOMS' AS INTEGER) = %s OR CAST(data->>'BedroomsTotal' AS INTEGER) = %s)")
                params.extend([criteria['bedrooms'], criteria['bedrooms']])
            
            if criteria.get('agent_id'):
                where_conditions.append("data->'_raw_data'->>'LIST_AGENT' = %s")
                params.append(criteria['agent_id'])
            
            # Construct query
            base_query = "SELECT listing_key, data, updated_at FROM listings"
            if where_conditions:
                query = f"{base_query} WHERE {' AND '.join(where_conditions)} ORDER BY updated_at DESC LIMIT 50"
            else:
                query = f"{base_query} ORDER BY updated_at DESC LIMIT 50"
            
            cur.execute(query, params)
            results = cur.fetchall()
            
            # Format results
            search_results = []
            for row in results:
                formatted = format_listing_data(row)
                if formatted:
                    search_results.append({"data": formatted})
            
            log.info(f"Advanced search returned {len(search_results)} listings")
            return search_results
            
    except Exception as e:
        log.error(f"Database error in advanced_search: {e}")
        raise HTTPException(status_code=500, detail="Database error")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)