import axios from 'axios';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3001';

export const apiClient = axios.create({
    baseURL: API_BASE_URL,
    headers: {
        'Content-Type': 'application/json',
    },
});

export interface ChatMessage {
    role: 'user' | 'assistant';
    content: string;
    images?: Array<{
        url: string;
        caption: string;
    }>;
}

export interface ChatResponse {
    message: string;
    images?: Array<{
        url: string;
        caption: string;
    }>;
}

export const sendMessage = async (message: string): Promise<ChatResponse> => {
    try {
        const response = await apiClient.post<ChatResponse>('/api/chat', { message });
        return response.data;
    } catch (error) {
        console.error('Error sending message:', error);
        throw new Error('Failed to send message');
    }
};
