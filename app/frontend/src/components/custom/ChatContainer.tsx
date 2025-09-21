import { ScrollArea } from "@/components/ui/scroll-area"

type Props = {
    children: React.ReactNode
}

export default function ChatContainer({ children }: Props) {
    return (
        <ScrollArea className="flex-1 p-4">
            {children}
        </ScrollArea>
    )
}
