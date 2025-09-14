import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"

type Props = {
    value: string
    onChange: (v: string) => void
    onSend: () => void
    disabled?: boolean
}

export default function ChatInput({ value, onChange, onSend, disabled }: Props) {
    return (
        <form
            className="flex mt-4 space-x-2"
            onSubmit={(e) => {
                e.preventDefault()
                onSend()
            }}
        >
            <Input
                value={value}
                onChange={(e) => onChange(e.target.value)}
                placeholder="Ask something..."
                disabled={disabled}
            />
            <Button type="submit" disabled={disabled}>
                Send
            </Button>
        </form>
    )
}
