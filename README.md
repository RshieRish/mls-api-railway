# MLS API for Railway Deployment

This is a FastAPI-based MLS (Multiple Listing Service) API designed for deployment on Railway.

## Features

- RESTful API endpoints for MLS listings
- CORS enabled for frontend integration
- Health check endpoint
- Sample data for testing
- Production-ready deployment configuration

## API Endpoints

- `GET /` - API information
- `GET /health` - Health check
- `GET /listings` - Get all listings with optional filters
  - Query parameters: `city`, `min_price`, `max_price`, `bedrooms`, `limit`
- `GET /listings/featured` - Get featured listings
- `GET /listings/{listing_id}` - Get specific listing by ID
- `POST /search` - Advanced search (accepts JSON criteria)

## Deployment on Railway

### Option 1: Deploy from GitHub

1. Push this code to a GitHub repository
2. Go to [Railway](https://railway.app)
3. Click "New Project" → "Deploy from GitHub repo"
4. Select your repository
5. Railway will automatically detect the Python app and deploy it

### Option 2: Deploy using Railway CLI

1. Install Railway CLI:
   ```bash
   npm install -g @railway/cli
   ```

2. Login to Railway:
   ```bash
   railway login
   ```

3. Initialize and deploy:
   ```bash
   railway init
   railway up
   ```

### Environment Variables

No environment variables are required for the basic deployment. The app will run on the port provided by Railway via the `$PORT` environment variable.

## Local Development

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the application:
   ```bash
   python main.py
   ```

3. Access the API at `http://localhost:8000`

## Integration with Frontend

Once deployed on Railway, you'll get a URL like `https://your-app-name.railway.app`. Update your frontend's `PYTHON_API_URL` environment variable to point to this URL.

### Example Frontend Integration

```javascript
// In your frontend .env file
PYTHON_API_URL=https://your-app-name.railway.app
```

## Sample Data

The API includes sample MLS listings for Massachusetts properties including:
- Boston Back Bay Victorian
- Cambridge condo
- Dracut family home
- Lexington colonial
- Marblehead waterfront property

## Next Steps

After successful deployment:
1. Test the API endpoints
2. Update your frontend to use the Railway URL
3. Optionally add a database for persistent data storage
4. Implement real MLS data integration