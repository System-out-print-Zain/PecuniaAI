type Props = {
    role: "user" | "assistant"
    content: string
}

export default function MessageBubble({ role, content }: Props) {
    return (
        <div
            className={`my-2 p-2 rounded-lg max-w-xs break-words ${role === "user"
                ? "bg-blue-500 text-white self-end ml-auto"
                : "bg-gray-500 dark:bg-gray-700 dark:text-white self-start mr-auto"
                }`}
        >
            {content}
        </div>
    )
}
