import { ScrollArea } from "@/components/ui/scroll-area"

type Props = {
    children: React.ReactNode
}

export default function ChatContainer({ children }: Props) {
    return (
        <ScrollArea className="flex-1 border rounded-lg p-4 bg-gray-50 dark:bg-gray-900">
            {children}
        </ScrollArea>
    )
}
