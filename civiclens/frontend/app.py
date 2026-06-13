"""
CivicLens — Gradio Chat Interface (Gradio 6.x compatible)
Deployable on HuggingFace Spaces (free, always-on).
"""
import asyncio
import gradio as gr
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TITLE = "CivicLens — Community Resource Finder"
DESCRIPTION = """
**Find local resources in your language.**
Type your need in *any language* — food, shelter, legal help, healthcare, utilities.

*Encuentre recursos locales en su idioma · 在您的语言中查找本地资源 · ابحث عن الموارد المحلية بلغتك*
"""

EXAMPLES = [
    "I need fresh vegetables for my family in Chicago",
    "Necesito comida para mi familia en Chicago",
    "我需要在芝加哥找到食物银行",
    "Je cherche une aide alimentaire à Chicago",
    "أحتاج إلى مساعدة قانونية في شيكاغو",
    "I need help paying my electric bill in Chicago",
    "I am fleeing domestic violence and need shelter in Chicago",
    "Necesito ayuda con mi factura de electricidad en Chicago",
]

PLACEHOLDER = "Type your need in any language... / Escriba su necesidad en cualquier idioma..."


async def chat(message: str, history: list) -> str:
    if not message or not message.strip():
        return "Please type your need or question."
    try:
        from main import run_pipeline
        return await run_pipeline(message.strip())
    except Exception as e:
        return (
            f"I'm having trouble right now. Please call **211** — "
            f"it's a free, confidential helpline available 24/7 for local resources.\n\n"
            f"_(Error: {e})_"
        )


async def respond(message, history):
    if not message or not message.strip():
        yield history or [], ""
        return

    history = history or []
    history.append({"role": "user", "content": message})
    history.append({"role": "assistant", "content": "Finding resources for you..."})
    yield history, ""

    response = await chat(message, history)
    history[-1]["content"] = response
    yield history, ""


def build_interface() -> gr.Blocks:
    css = """
    .disclaimer { font-size: 0.8rem; color: #666; text-align: center; margin-top: 0.5rem; }
    footer { display: none !important; }
    """
    theme = gr.themes.Soft(primary_hue="blue", secondary_hue="green", neutral_hue="slate")

    with gr.Blocks(title=TITLE) as demo:
        gr.Markdown(f"# {TITLE}")
        gr.Markdown(DESCRIPTION)

        chatbot = gr.Chatbot(
            label="",
            height=460,
            show_label=False,
            avatar_images=(None, "https://em-content.zobj.net/source/apple/391/seedling_1f331.png"),
        )

        with gr.Row():
            msg_input = gr.Textbox(
                placeholder=PLACEHOLDER,
                show_label=False,
                scale=9,
                autofocus=True,
                lines=1,
            )
            send_btn = gr.Button("Send", scale=1, variant="primary")

        with gr.Row():
            clear_btn = gr.Button("Clear conversation", size="sm", variant="secondary")

        gr.Examples(
            examples=EXAMPLES,
            inputs=msg_input,
            label="Try these examples / Pruebe estos ejemplos",
        )

        gr.Markdown(
            "**211** · Free 24/7 helpline · Línea de ayuda gratuita 24/7",
            elem_classes="disclaimer",
        )

        msg_input.submit(respond, [msg_input, chatbot], [chatbot, msg_input])
        send_btn.click(respond, [msg_input, chatbot], [chatbot, msg_input])
        clear_btn.click(lambda: ([], ""), outputs=[chatbot, msg_input])

    return demo, theme, css


if __name__ == "__main__":
    demo, theme, css = build_interface()
    demo.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False,
        theme=theme,
        css=css,
    )
