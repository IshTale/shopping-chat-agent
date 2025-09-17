import axios from 'axios';

const GEMINI_API_URL = 'https://api.gemini.com';
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

export const generateResponse = async (message: string) => {
    if (!GEMINI_API_KEY) {
        throw new Error('GEMINI_API_KEY is not set in the environment variables');
    }

    try {
        const response = await axios.post(
            `${GEMINI_API_URL}/generate`,
            { message },
            {
                headers: {
                    Authorization: `Bearer ${GEMINI_API_KEY}`,
                },
            }
        );

        return response.data;
    } catch (error) {
        console.error('Error generating response:', error);
        throw error;
    }
};
