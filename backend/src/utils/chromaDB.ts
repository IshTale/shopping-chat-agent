import { ChromaClient } from 'chromadb';

const CHROMADB_PATH = process.env.CHROMADB_PATH;

if (!CHROMADB_PATH) {
    throw new Error('CHROMADB_PATH is not set in the environment variables');
}

const client = new ChromaClient({ path: CHROMADB_PATH });

export const queryChromaDB = async (query: string) => {
    try {
        const results = await client.query({ query });
        return results;
    } catch (error) {
        console.error('Error querying ChromaDB:', error);
        throw error;
    }
};
