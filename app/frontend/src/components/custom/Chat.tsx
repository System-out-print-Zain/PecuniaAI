import { useState, useRef, useEffect } from "react"
import ChatContainer from "@/components/custom/ChatContainer"
import MessageBubble from "@/components/custom/MessageBubble"
import ChatInput from "@/components/custom/ChatInput"
import ChatLoader from "@/components/custom/ChatLoader"

type Message = {
    role: "user" | "assistant"
    content: string
}

export default function Chat() {
    const [messages, setMessages] = useState<Message[]>([])
    const [input, setInput] = useState("")
    const [loading, setLoading] = useState(false)
    const scrollRef = useRef<HTMLDivElement>(null)

    useEffect(() => {
        scrollRef.current?.scrollIntoView({ behavior: "smooth" })
    }, [messages, loading])

    const sendMessage = async () => {
        if (!input.trim()) return
        const userMessage = { role: "user", content: input }
        setMessages((prev) => [...prev, userMessage])
        setInput("")
        setLoading(true)

        // --- call your RAG backend ---
        const res = await fetch("http://localhost:8000/chat", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ query: input }),
        })
        const data = await res.json()
        setMessages((prev) => [...prev, { role: "assistant", content: data.answer }])
        setLoading(false)
    }

    return (
        <div className="flex flex-col h-screen max-w-2xl mx-auto p-4">
            <ChatContainer>
                {messages.map((m, i) => (
                    <MessageBubble key={i} role={m.role} content={m.content} />
                ))}
                {loading && <ChatLoader />}
                <div ref={scrollRef} />
            </ChatContainer>

            <ChatInput value={input} onChange={setInput} onSend={sendMessage} disabled={loading} />
        </div>
    )
}
