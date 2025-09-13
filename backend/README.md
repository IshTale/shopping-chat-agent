# Backend for Chat Application

This backend is built using Node.js, TypeScript, and Express.js. It integrates with the Gemini API, Hugging Face embedding models, and ChromaDB for retrieval-augmented generation (RAG). The backend also retrieves and serves images with captions.

## Features
- Chat endpoint for handling user input and generating responses.
- Integration with Gemini API for text generation.
- Custom embeddings using Hugging Face models.
- Retrieval-augmented generation (RAG) with ChromaDB.
- Image retrieval and serving with captions.

## Getting Started

### Prerequisites
- Node.js (v16+)
- npm or yarn

### Installation
1. Clone the repository:
   ```bash
   git clone <repository-url>
   ```
2. Navigate to the project directory:
   ```bash
   cd backend
   ```
3. Install dependencies:
   ```bash
   npm install
   ```

### Running the Server
To start the development server, run:
```bash
npm run dev
```

### Project Structure
- `src/`: Contains the source code.
- `src/routes/`: API routes.
- `src/services/`: Services for interacting with external APIs and databases.
- `src/utils/`: Utility functions.

### Environment Variables
Create a `.env` file in the root directory and add the following variables:
```
GEMINI_API_KEY=your-gemini-api-key
CHROMADB_PATH=path-to-chromadb
```

### License
This project is licensed under the MIT License.
