import { useState, useRef, useEffect } from "react"
import ChatContainer from "@/components/custom/ChatContainer"
import MessageBubble from "@/components/custom/MessageBubble"
import ChatInput from "@/components/custom/ChatInput"
import ChatLoader from "@/components/custom/ChatLoader"
import './App.css'

type Message = {
  role: "user" | "assistant"
  content: string
}

function App() {
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState("")
  const [loading, setLoading] = useState(false)
  const scrollRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    scrollRef.current?.scrollIntoView({ behavior: "smooth" })
  }, [messages, loading])

  const sendMessage = async () => {
    if (!input.trim()) return
    const userMessage: Message = { role: "user", content: input }
    setMessages((prev) => [...prev, userMessage])
    setInput("")
    setLoading(true)

    try {
      const res = await fetch("http://localhost:8000/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: input }),
      })
      const data = await res.json()
      const assistantMessage: Message = { role: "assistant", content: data.answer }
      setMessages((prev) => [...prev, assistantMessage])
    } catch (err) {
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: "Error: Could not get response." },
      ])
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="flex flex-col h-screen max-w-2xl mx-auto p-4">

      {/* Sticky Header */}
      <header className="sticky top-0 z-20 bg-gray-800 text-white py-4 px-6 rounded-lg shadow-md">
        <h1 className="text-xl font-bold">Pecunia AI</h1>
        <p className="text-sm text-gray-300">Your AI-powered financial assistant</p>
      </header>

      {/* Chat area */}
      <div className="flex-1 flex flex-col mt-4 overflow-hidden">
        <ChatContainer>
          {messages.map((msg, idx) => (
            <MessageBubble key={idx} role={msg.role} content={msg.content} />
          ))}
          {loading && <ChatLoader />}
          <div ref={scrollRef} />
        </ChatContainer>

        <ChatInput
          value={input}
          onChange={setInput}
          onSend={sendMessage}
          disabled={loading}
        />
      </div>
    </div>
  )
}

export default App
