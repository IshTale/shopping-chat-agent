'use client';

import { useState } from 'react';
import ChatMessage from '@/components/ChatMessage';
import ChatInput from '@/components/ChatInput';

interface Message {
    id: string;
    content: string;
    role: 'user' | 'assistant';
    images?: { url: string; caption: string }[];
}

export default function Home() {
    const [messages, setMessages] = useState<Message[]>([]);

    const handleSendMessage = async (message: string) => {
        const newMessage: Message = {
            id: Date.now().toString(),
            content: message,
            role: 'user',
        };

        setMessages((prev) => [...prev, newMessage]);

        // TODO: Add API call here
        // Simulate assistant response
        const assistantMessage: Message = {
            id: (Date.now() + 1).toString(),
            content: 'This is a sample response with an image.',
            role: 'assistant',
            images: [
                {
                    url: '/placeholder-image.jpg',
                    caption: 'Sample image caption',
                },
            ],
        };

        setMessages((prev) => [...prev, assistantMessage]);
    };

    return (
        <main className="flex flex-col h-screen">
            <div className="flex-1 overflow-y-auto p-4 space-y-4">
                {messages.map((message) => (
                    <ChatMessage key={message.id} message={message} />
                ))}
            </div>
            <div className="p-4 border-t border-gray-600">
                <ChatInput onSendMessage={handleSendMessage} />
            </div>
        </main>
    );
}
