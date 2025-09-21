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
            className="flex mt-4 space-x-5"
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
            <Button className="text-white bg-black" type="submit" disabled={disabled}>
                Ask
            </Button>
        </form>
    )
}
