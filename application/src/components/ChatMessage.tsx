'use client';

import Image from 'next/image';
import { Message } from '@/types/chat';

interface ChatMessageProps {
    message: Message;
}

export default function ChatMessage({ message }: ChatMessageProps) {
    const isAssistant = message.role === 'assistant';

    return (
        <div className={`flex ${isAssistant ? 'bg-message-bg' : ''} p-4 rounded-lg`}>
            <div className={`flex flex-col ${isAssistant ? 'items-start' : 'items-end'} w-full`}>
                <div className="prose text-white max-w-none">
                    {message.content}
                </div>
                {message.images && message.images.length > 0 && (
                    <div className="mt-4 grid gap-4 grid-cols-1 sm:grid-cols-2">
                        {message.images.map((image, index) => (
                            <div key={index} className="relative">
                                <div className="relative aspect-square w-full">
                                    <Image
                                        src={image.url}
                                        alt={image.caption}
                                        fill
                                        className="object-cover rounded-lg"
                                    />
                                </div>
                                {image.caption && (
                                    <p className="mt-2 text-sm text-gray-300">
                                        {image.caption}
                                    </p>
                                )}
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
}
